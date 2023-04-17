""""""
import os
import json
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, Tuple, Callable, TypeVar
from etl.helper_functions import get_connection, get_config, wrap_with_timings
from sqlalchemy import text, CursorResult, TextClause


QRT = TypeVar('QRT')


@dataclass
class RuntimeBenchmarkResult:
    result: CursorResult
    time_taken: float
    benchmark_id: int
    benchmark_name: str

    def __init__(self, result: CursorResult, time_taken: float, benchmark_id: int, benchmark_name: str) -> None:
        self.result = result
        self.time_taken = time_taken
        self.benchmark_id = benchmark_id
        self.benchmark_name = benchmark_name




# Lau gave me a thought that I should make the hierarchy a step deeper and then have a Runtime Benchmark Runner
class AbstractBenchmarkRunner(ABC):
    """"""
    def __init__(self, garbage_queries_folder: str, garbage_queries_per_iteration: int = 10, iterations: int = 10) -> None:
        self.config = get_config()
        self._conn = get_connection(self.config, auto_commit_connection=True)
        self.garbage_queries_per_iteration = garbage_queries_per_iteration
        self.iterations = iterations
        self.garbage_queries_folder = garbage_queries_folder


    @abstractmethod
    def _get_benchmarks_to_run(self) -> Dict[str, Callable[[], QRT]]:
        raise NotImplementedError  # To be implemented by subclasses


    def run_benchmark(self) -> None:
        """"""
        benchmarks = self._get_benchmarks_to_run()
        for name, executable in benchmarks.items():
            for i in range(self.iterations):
                result  = wrap_with_timings(f'Running benchmark <{name}> iteration <{i+1}> ', executable)
                wrap_with_timings(f'Storing result for benchmar <{name}> iteration <{i+1}>', lambda: self._store_result(i+1, result))


    @abstractmethod
    def _store_result(self, iteration: int, result: QRT) -> None:
        raise NotImplementedError  # To be implemented by subclasses


    def _get_next_test_id(self) -> int:
        """"""
        result_cursor = self._conn.execute(text("SELECT nextval('benchmark_results_id_seq');"))
        return result_cursor.fetchone()[0]


    def _execute(self, query: TextClause, params: Dict[str, any] = {}) -> CursorResult:
        return self._conn.execute(query, parameters=params)


    def _get_queries_in_folder(self, folder: str) -> Dict[str, str]:
        """"""
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


class AbstractRuntimeBenchmarkRunner(AbstractBenchmarkRunner, ABC):
    """"""
    QUERY_NAME_KEY = 'name'
    BENCHMARK_ID_KEY = 'id'

    def __init__(self, garbage_queries_folder: str, garbage_queries_per_iteration: int = 10, iterations: int = 10) -> None:
        super().__init__(garbage_queries_folder, garbage_queries_per_iteration, iterations)


    def _store_result(self, iteration: int, result: RuntimeBenchmarkResult) -> None:
        """"""
        json_result = result.result.fetchone()[0][0]
        data = json.dumps(json_result)
        
        self._conn.execute(
            text("""
                INSERT INTO benchmark_results
                (test_run_id, query_name, iteration, explain, execution_time_ms)
                VALUES (:id, :name, :it, :result, :time)
            """),
            {'id': result.benchmark_id, 'name': result.benchmark_name, 'it': iteration, 'result': data, 'time': result.time_taken}
        )
