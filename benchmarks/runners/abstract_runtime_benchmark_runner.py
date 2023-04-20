"""Module containing the abstract runtime benchmark runner which all runtime benchmark runners inherit from."""
import json
from abc import ABC
from sqlalchemy import text
from benchmarks.runners.abstract_benchmark_runner import AbstractBenchmarkRunner
from benchmarks.dataclasses.runtime_benchmark_result import RuntimeBenchmarkResult


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
        self._query_prefix = 'explain (analyze, timing, format json, verbose, buffers, settings)'

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
