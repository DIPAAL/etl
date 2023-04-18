"""Module containing abstract classes used for implementing benchmark runners."""
import os
import json
import time
import random
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, Callable, TypeVar
from etl.helper_functions import get_connection, get_config, wrap_with_timings
from sqlalchemy import text, CursorResult, TextClause, Connection


# User-defined type used for implementing generics (BRT = Benchmark Result Type)
BRT = TypeVar('BRT')


@dataclass
class RuntimeBenchmarkResult:
    """Dataclass for storing the result of a runtime benchmark."""

    result: CursorResult
    time_taken: float
    benchmark_id: int
    benchmark_name: str


class AbstractBenchmarkRunner(ABC):
    """Abstract superclass that all benchmark runners should inherit from."""

    def __init__(self, garbage_queries_folder: str, garbage_queries_per_iteration: int = 10, iterations: int = 10) \
            -> None:
        """
        Initialize an abstract benchmark runner.

        Arguments:
            garbage_queries_folder: path to folder containing queries for prewarming the OS cache
            garbage_queries_per_iteration: how many queries should be run to prewarm cache (default: 10)
            iterations: how many times should each query in the benchmark be repeated (default: 10)
        """
        self.config = get_config()
        self.__setup_benchmark_connection()
        self.garbage_queries_per_iteration = garbage_queries_per_iteration
        self.iterations = iterations
        self.garbage_queries_folder = garbage_queries_folder
        self._local_run = True if os.getenv('tag', 'local_dev') == 'local_dev' else False

    @abstractmethod
    def _get_benchmarks_to_run(self) -> Dict[str, Callable[[], BRT]]:
        raise NotImplementedError  # To be implemented by subclasses

    def run_benchmark(self) -> None:
        """Run the benchmark defined in the benchmark runner."""
        benchmarks = self._get_benchmarks_to_run()
        for name, executable in benchmarks.items():
            for i in range(self.iterations):
                wrap_with_timings('Cache prewarming', lambda: self.__prewarm_cache())
                result = wrap_with_timings(f'Running benchmark <{name}> iteration <{i+1}> ', executable)
                wrap_with_timings(
                    f'Storing result for benchmark <{name}> iteration <{i+1}>',
                    lambda: self._store_result(i+1, result)
                )

    def __prewarm_cache(self) -> None:
        """Prewarm the data warehouse cache befire running benchmarks."""
        # If run locally do not attempt to clear OS cache
        if not self._local_run:
            wrap_with_timings('Clearing os cache', lambda: self.__clear_cache())

        print(f'Running {self.garbage_queries_per_iteration} garbage queries before next iteration')
        garbage_queries = self._get_queries_in_folder(self.garbage_queries_folder)
        random_queries = random.choices(list(garbage_queries.values()), k=self.garbage_queries_per_iteration)
        for i in range(len(random_queries)):
            wrap_with_timings(f'   Executing garbage query <{i+1}>', lambda: self._execute(text(random_queries[i])))
        print('Finished running garbage queries')

    def __clear_cache(self) -> None:
        """
        Clear the OS cache of the Citus cluster.

        NOTE: This method should never be run locally.
        """
        while True:
            try:
                self._conn.close()
                exit_code = os.system('bash benchmarks/clear_cache.sh')
                if exit_code != 0:
                    raise Exception('Clearing cache failed!')

                self.__setup_benchmark_connection()

                break
            except Exception as e:
                print(f'Exception thrown while clearing cache, trying again in 5 seconds <{e}>')
                time.sleep(5)
                continue

    @abstractmethod
    def _store_result(self, iteration: int, result: BRT) -> None:
        raise NotImplementedError  # To be implemented by subclasses

    def _get_next_test_id(self) -> int:
        """Fetch the next benchmark id from data warehouse."""
        result_cursor = self._conn.execute(text("SELECT nextval('benchmark_results_id_seq');"))
        return result_cursor.fetchone()[0]

    def _execute(self, query: TextClause, params: Dict[str, any] = {}) -> CursorResult:
        """
        Execute queries with parameters.

        Arguments:
            query: the query to execute
            params: dictionary containing key-value pairs defining the parameters for the query (default: {})
        """
        return self._conn.execute(query, parameters=params)

    def _get_queries_in_folder(self, folder: str) -> Dict[str, str]:
        """
        Get the sql query file containts in folder.

        Arguments:
            folder: the folder containing the .sql files
        """
        files = [f for f in os.listdir(folder) if f.endswith('.sql')]

        # return contents as dict filename -> query
        return {f: open(os.path.join(folder, f), 'r').read() for f in files}

    def _get_queries_in_folder_and_subfolder(self, folder: str) -> Dict[str, str]:
        """
        Recursively get all sql files in folder.

        Arguments:
            folder: The parent folder to recursively traverse
        """
        files = [f for _, _, f in os.walk(folder) if len(f) > 0]
        files = [sql_file_name for sub_folder_list in
                 files for sql_file_name in sub_folder_list if sql_file_name.endswith('.sql')]

        return {f: open(os.path.join(folder, f), 'r').read() for f in files}

    def __setup_benchmark_connection(self) -> Connection:
        """Set up connection for running benchmarks."""
        self._conn = get_connection(self.config, auto_commit_connection=True)
        # Enable explaining all tasks if not already set.
        self._conn.execute(text('SET citus.explain_all_tasks = 1;'))


class AbstractRuntimeBenchmarkRunner(AbstractBenchmarkRunner, ABC):
    """Abstract superclass that all runtime benchmark runners should inherit from."""

    QUERY_NAME_KEY = 'name'
    BENCHMARK_ID_KEY = 'id'

    def __init__(self, garbage_queries_folder: str, garbage_queries_per_iteration: int = 10, iterations: int = 10) \
            -> None:
        """
        Initialize an abstract runtime benchmark runner.

        Arguments:
            garbage_queries_folder: path to folder containing queries for prewarming the OS cache
            garbage_queries_per_iteration: how many queries should be run to prewarm cache (default: 10)
            iterations: how many times should each query in the benchmark be repeated (default: 10)
        """
        super().__init__(garbage_queries_folder, garbage_queries_per_iteration, iterations)

    def _store_result(self, iteration: int, result: RuntimeBenchmarkResult) -> None:
        """
        Store the result of running the benchmark.

        Arguments:
            iteration: the iteration of the benchmark
            result: all the results of running the particular benchmark
        """
        json_result = result.result.fetchone()[0][0]
        data = json.dumps(json_result)

        self._conn.execute(
            text("""
                INSERT INTO benchmark_results
                (test_run_id, query_name, iteration, explain, execution_time_ms)
                VALUES (:id, :name, :it, :result, :time)
            """),
            {'id': result.benchmark_id, 'name': result.benchmark_name,
             'it': iteration, 'result': data, 'time': result.time_taken}
        )
