"""Module for running benchmarks for query performance."""
import json
import os
import random
from time import perf_counter

from typing import List

from etl.helper_functions import get_connection, wrap_with_timings


class BenchmarkRunner:
    """Class to run benchmarks."""

    def __init__(self, config, number_garbage_queries_between=10, iterations=10):
        """
        Init a benchmark runner.

        Keyword arguments:
            config -- a dict containing the connection parameters
            number_garbage_queries_between -- number of garbage queries to run between each benchmark query
            iterations -- number of iterations to run each benchmark query
        """
        self._config = config
        self._number_garbage_queries_between = number_garbage_queries_between
        self._iterations = iterations
        self._conn = get_connection(config)
        self._conn.set_session(autocommit=True)
        self._garbage_queries = self._get_garbage_queries()

    def run_benchmark(self):
        """
        Run a benchmark on all sql queries defined in the benchmarks/queries folder.

        Run garbage queries inbetween to keep cache lukewarm.
        Configurable iterations and garbage queries between in constructor.
        """
        test_run_id = self._get_test_run_id(self._conn)
        print(f'Test run id: {test_run_id}')

        # Enable explaining all tasks if not already set.
        query = 'SET citus.explain_all_tasks = 1;'
        self._conn.cursor().execute(query)

        # get queries to benchmark
        queries = self._get_queries_to_benchmark()
        # sort by key ascending
        queries = {k: queries[k] for k in sorted(queries)}

        for query_name, query in queries.items():
            for i in range(self._iterations):
                self._run_random_garbage_queries()

                query = f'explain (analyze, timing, format json, verbose, buffers, settings) \n{query}'

                cursor = self._conn.cursor()

                start = perf_counter()
                wrap_with_timings(f'Running query {query_name} iteration {i}', lambda: cursor.execute(query))
                end = perf_counter()
                time_taken_ms = int((end - start) * 1000)

                result = cursor.fetchone()[0][0]
                # encode dict to json
                result = json.dumps(result)

                query = """
                    INSERT INTO benchmark_results (test_run_id, query_name, iteration, explain, execution_time_ms)
                    VALUES (%s, %s, %s, %s, %s)
                """
                self._conn.cursor().execute(query, (test_run_id, query_name, i, result, time_taken_ms))

    def _run_random_garbage_queries(self):
        for i in range(self._number_garbage_queries_between):
            # pick random garbage query
            garbage_query = random.choice(self._garbage_queries)
            wrap_with_timings(f'Garbage Query {i}', lambda: self._conn.cursor().execute(garbage_query))

    def _get_queries_to_benchmark(self):
        folder = 'benchmarks/queries'
        return self._get_queries_in_folder(folder)

    def _get_garbage_queries(self) -> List[str]:
        folder = 'benchmarks/garbage_queries'
        return list(self._get_queries_in_folder(folder).values())

    def _get_queries_in_folder(self, folder):
        files = [f for f in os.listdir(folder) if f.endswith('.sql')]

        # return contents as dict filename -> query
        return {f: open(os.path.join(folder, f), 'r').read() for f in files}

    def _get_test_run_id(self, conn):
        cursor = conn.cursor()
        cursor.execute("SELECT nextval('benchmark_results_id_seq');")
        return cursor.fetchone()[0]
