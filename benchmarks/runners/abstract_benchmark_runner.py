"""Module containing the abstract benchmark runner which all benchmark runners inherit from."""
import os
import random
import time
from abc import ABC, abstractmethod
from typing import Dict, Callable, TypeVar
from etl.helper_functions import get_config, wrap_with_timings, get_connection
from sqlalchemy import text, TextClause, CursorResult
from benchmarks.errors.cache_clearing_error import CacheClearingError


# User-defined type used for implementing generics (BRT = Benchmark Result Type)
BRT = TypeVar('BRT')


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
        self._conn.close()
        while True:
            try:
                exit_code = os.system('bash benchmarks/clear_cache.sh')
                if exit_code != 0:
                    raise CacheClearingError(exit_code, f'Clearing cache failed! with exit code <{exit_code}>')

                self.__setup_benchmark_connection()
                break
            except CacheClearingError as e:
                print('Exception raised while clearing cache'
                      f' trying again in 5 seconds <{e}>')
                time.sleep(5)
                continue

        self.__setup_benchmark_connection()

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

    def __setup_benchmark_connection(self, retry_interval_sec: int = 5, max_num_retries: int = -1) -> None:
        """
        Set up connection for running benchmarks.

        Arguments:
            retry_interval_sec: amount of seconds to wait before re-trying to connect to data warehouse (default: 5)
            man_num_retries: amount of failed retries before stopping, negative value = indefinite (default: -1)
        """
        fail_cnt = 0
        while True:
            try:
                self._conn = get_connection(self.config, auto_commit_connection=True)
                # Enable explaining all tasks if not already set.
                self._conn.execute(text('SET citus.explain_all_tasks = 1;'))
                break
            except Exception as e:
                fail_cnt += 1
                print(f'Failed attempt <{fail_cnt}> at setting up connection to data warehouse, with exception <{e}>.')
                if max_num_retries < 0 or fail_cnt < max_num_retries:
                    print(f'Re-trying to establish connection to data warehouse in <{retry_interval_sec}> seconds')
                    time.sleep(retry_interval_sec)
                else:
                    # Last retry reached, re-raise exception
                    raise e
