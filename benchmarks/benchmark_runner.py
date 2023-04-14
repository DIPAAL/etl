""""""
import os
from abc import ABC, abstractmethod
from typing import Dict, Callable
from etl.helper_functions import get_connection, get_config, wrap_with_timings
from sqlalchemy import text, CursorResult, TextClause

class AbstractBenchmarkRunner(ABC):
    def __init__(self, garbage_queries_folder: str, garbage_queries_per_iteration: int = 10, iterations: int = 10) -> None:
        self.config = get_config()
        self._conn = get_connection(self.config, auto_commit_connection=True)
        self.garbage_queries_per_iteration = garbage_queries_per_iteration
        self.iterations = iterations
        self.garbage_queries_folder = garbage_queries_folder


    @abstractmethod
    def _get_benchmarks_to_run(self) -> Dict[str, Callable[[int], None]]:
        raise NotImplementedError  # To be implemented by subclasses


    def run_benchmark(self) -> None:
        benchmarks = self._get_benchmarks_to_run()
        for name, executable in benchmarks:
            for i in range(self.iterations):
                wrap_with_timings(f'Running benchmark <{name}> iteration <{i}> ',
                                  lambda: executable(i))


    def _get_next_test_id(self) -> int:
        result_cursor = self._conn.execute(text("SELECT nextval('benchmark_results_id_seq');"))
        return result_cursor.fetchone()[0]


    def _execute(self, query: TextClause, params: Dict[str, any] = {}) -> CursorResult:
        return self._conn.execute(query, parameters=params)


    def _get_queries_in_folder(self, folder: str) -> Dict[str, str]:
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
        files = [sql for sublist in files for sql in sublist if sql.endswith('.sql')]

        return {f: open(os.path.join(folder, f), 'r').read() for f in files}
