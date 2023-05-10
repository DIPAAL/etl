"""Module containing the abstract runtime benchmark runner which all runtime benchmark runners inherit from."""
import json
from abc import ABC
from sqlalchemy import text
from typing import Dict, Any
from etl.helper_functions import measure_time
from benchmarks.runners.abstract_benchmark_runner import AbstractBenchmarkRunner
from benchmarks.dataclasses.runtime_benchmark_result import RuntimeBenchmarkResult


class AbstractRuntimeBenchmarkRunner(AbstractBenchmarkRunner, ABC):
    """Abstract superclass that all runtime benchmark runners should inherit from."""

    def __init__(self, iterations: int = 10) \
            -> None:
        """
        Initialize an abstract runtime benchmark runner.

        Arguments:
            iterations: how many times should each query in the benchmark be repeated (default: 10)
        """
        super().__init__(iterations)
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
                (test_run_id, type, query_name, iteration, explain, execution_time_ms)
                VALUES (:id, :type, :name, :it, :result, :time)
            """),
            {'id': result.benchmark_id, 'name': result.benchmark_name,
             'it': iteration, 'result': data, 'time': result.time_taken,
             'type': result.benchmark_type}
        )

    def _execute_runtime_benchmark(self, id: int, params: Dict[str, Any], query: str, name: str, type: str)\
            -> RuntimeBenchmarkResult:
        """
        Wrap runtime benchmark execution.

        Arguments:
            id: the test run id of the benchmark
            params: the parameters to run the benchmark query with
            query: the benchmark query to run
            name: the name of the benchmark
            type: the type of the benchmark
        """
        return RuntimeBenchmarkResult(
            *measure_time(lambda: self._conn.execute(text(query), parameters=params)),
            id,
            name,
            type
        )
