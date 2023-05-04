"""Module containing the abstract benchmark runner which all benchmark runners inherit from."""
import os
import random
from abc import ABC, abstractmethod
from typing import Dict, Callable, TypeVar
from etl.helper_functions import get_config, wrap_with_timings, get_connection, get_staging_cell_sizes, \
    get_first_query_in_file, extract_smart_date_id_from_date, extract_smart_time_id_from_date, \
    wrap_with_retry_and_timing
from benchmarks.errors.cache_clearing_error import CacheClearingError
from sqlalchemy import text
from datetime import datetime


# User-defined type used for implementing generics (BRT = Benchmark Result Type)
BRT = TypeVar('BRT')


class AbstractBenchmarkRunner(ABC):
    """Abstract superclass that all benchmark runners should inherit from."""

    def __init__(self, iterations: int = 10) \
            -> None:
        """
        Initialize an abstract benchmark runner.

        Arguments:
            iterations: how many times should each query in the benchmark be repeated (default: 10)
        """
        self._config = get_config()
        wrap_with_retry_and_timing('Setup benchmarking connection', lambda: self._setup_benchmark_connection())
        self._garbage_queries_iterations = 3
        self._garbage_start_period_timestamp = datetime(year=2021, month=1, day=1)
        self._garbage_end_period_timestamp = datetime(year=2022, month=1, day=1)
        self._iterations = iterations
        self._garbage_queries_folder = 'benchmarks/garbage_queries'
        self._local_run = True if os.getenv('tag', 'local_dev') == 'local_dev' else False
        self._available_resolutions = get_staging_cell_sizes()

    @abstractmethod
    def _get_benchmarks_to_run(self) -> Dict[str, Callable[[], BRT]]:
        raise NotImplementedError  # To be implemented by subclasses

    def run_benchmark(self) -> None:  # noqa: C901
        """Run the benchmark defined in the benchmark runner."""
        benchmarks = self._get_benchmarks_to_run()
        for name, executable in benchmarks.items():
            for i in range(self._iterations):
                wrap_with_retry_and_timing('Benchmark iteration',
                                           lambda: self._run_benchmark_iteration(name, i+1, executable))

    def _prewarm_cache(self) -> None:
        """Prewarm the data warehouse cache befire running benchmarks."""
        # If run locally do not attempt to clear OS cache
        if not self._local_run:
            wrap_with_retry_and_timing('Clearing os cache', lambda: self._clear_cache())

        wrap_with_timings('Garbage queries', lambda: self._run_garbage_queries())

    def _run_garbage_queries(self) -> None:
        """Run defined garbage queries."""
        garbage_queries = self._get_queries_in_folder(self._garbage_queries_folder)
        longest_key = len(max(garbage_queries.keys(), key=len))
        garbage_queries = [(name[:-4], val) for name, val in garbage_queries.items()]
        random_params_query = get_first_query_in_file('benchmarks/queries/misc/random_garbage_parameters.sql')
        for _ in range(self._garbage_queries_iterations):
            random.shuffle(garbage_queries)
            for i in range(len(garbage_queries)):
                name, query = garbage_queries[i]
                random_parameters = self._conn.execute(text(random_params_query), parameters={
                    'period_start_timestamp': self._garbage_start_period_timestamp,
                    'period_end_timestamp': self._garbage_end_period_timestamp
                }).fetchone()._asdict()
                random_parameters = random_parameters | {
                    'start_date_id': extract_smart_date_id_from_date(random_parameters['start_time']),
                    'end_date_id': extract_smart_date_id_from_date(random_parameters['end_time']),
                    'start_time_id': extract_smart_time_id_from_date(random_parameters['start_time']),
                    'end_time_id': extract_smart_time_id_from_date(random_parameters['end_time'])
                }

                query = query.format(CELL_SIZE=random_parameters['spatial_resolution'])
                wrap_with_timings(f'   Executing garbage query <{i+1}> <{name.ljust(longest_key - 4)}>',
                                  lambda: self._conn.execute(text(query), parameters=random_parameters))

    def _run_benchmark_iteration(self, name: str, iteration: int, executable: Callable[[], BRT]) -> None:
        """
        Execute the iteration of a benchmark.

        Arguments:
            name: name of the benchmark
            iteration: number indicating the current iteration
            executable: execute to run the benchmark
        """
        wrap_with_timings('Cache prewarming', lambda: self._prewarm_cache())
        result = wrap_with_timings(f'Running benchmark <{name}> iteration <{iteration}> ', executable)
        wrap_with_timings(
            f'Storing result for benchmark <{name}> iteration <{iteration}>',
            lambda: self._store_result(iteration, result)
        )

    def _clear_cache(self) -> None:
        """
        Clear the OS cache of the Citus cluster.

        NOTE: This method should never be run locally.
        """
        if not self._conn.closed:
            self._conn.close()
        exit_code = os.system('bash benchmarks/clear_cache.sh')
        if exit_code != 0:
            raise CacheClearingError(exit_code, f'Clearing cache failed! with exit code <{exit_code}>')

        wrap_with_retry_and_timing('Setup benchmarking connection', lambda: self._setup_benchmark_connection())

    @abstractmethod
    def _store_result(self, iteration: int, result: BRT) -> None:
        raise NotImplementedError  # To be implemented by subclasses

    def _get_next_test_id(self) -> int:
        """Fetch the next benchmark id from data warehouse."""
        result_cursor = self._conn.execute(text("SELECT nextval('test_run_id_seq');"))
        return result_cursor.fetchone()[0]

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

    def _setup_benchmark_connection(self) -> None:
        """Set up connection for running benchmarks."""
        self._conn = get_connection(self._config, auto_commit_connection=True)
        # Enable explaining all tasks if not already set.
        self._conn.execute(text('SET citus.explain_all_tasks = 1;'))
