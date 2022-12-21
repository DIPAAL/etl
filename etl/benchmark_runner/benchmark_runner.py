"""Module for running benchmarks for query performance."""
import json
import os
from time import perf_counter
import time

from typing import List

from etl.helper_functions import get_connection, wrap_with_timings


class BenchmarkRunner:
    """
    Class to run benchmarks.

    Methods
    -------
    run_benchmark(): run a benchmark on all sql queries defined in the benchmarks/queries folder
    """

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
        self._config = config
        self._garbage_queries = self._get_garbage_queries()

    def run_benchmark(self):
        """
        Run a benchmark on all sql queries defined in the benchmarks/queries folder.

        Run garbage queries inbetween to keep cache lukewarm.
        Configurable iterations and garbage queries between in constructor.
        """
        conn = get_connection(self._config)
        conn.set_session(autocommit=True)

        test_run_id = self._get_test_run_id(conn)
        print(f'Test run id: {test_run_id}')

        # get queries to benchmark
        queries = self._get_queries_to_benchmark()
        # sort by key ascending
        queries = {k: queries[k] for k in sorted(queries)}

        for i in range(self._iterations):
            for query_name, query in queries.items():
                self._run_query(query_name, query, test_run_id, i)

    def _run_query(self, query_name, query, test_run_id, iteration):  # noqa: C901
        while True:
            try:
                # Clear the cache by running clear_cache.sh
                exit_code = os.system('bash benchmarks/clear_cache.sh')
                if exit_code != 0:
                    raise Exception('Clearing cache failed')

                # Try to connect to the database, if it fails, try again in 5 seconds
                conn = self._retry_get_connection()

                # Enable explaining all tasks if not already set.
                query = 'SET citus.explain_all_tasks = 1;'
                conn.cursor().execute(query)

                cursor = conn.cursor()

                start = perf_counter()
                wrap_with_timings(f'Running query {query_name} iteration {iteration}', lambda: cursor.execute(query))
                end = perf_counter()
                time_taken_ms = int((end - start) * 1000)

                result = cursor.fetchone()[0][0]
                # encode dict to json
                result = json.dumps(result)

                conn.cursor().execute("""
                        INSERT INTO benchmark_results
                        (test_run_id, query_name, iteration, explain, execution_time_ms)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (test_run_id, query_name, iteration, result, time_taken_ms))
                break
            except Exception as e:
                print(f'Exception thrown while running query, trying again: {e}')

    def _get_queries_to_benchmark(self):
        folder = 'benchmarks/queries'
        return self._get_queries_in_folder(folder)

    def _get_garbage_queries(self) -> List[str]:
        folder = 'benchmarks/garbage_queries'
        return list(self._get_queries_in_folder(folder).values())

    def _get_queries_in_folder(self, folder):
        files = [f for f in os.listdir(folder) if f.endswith('.sql')]

        # return contents as dict filename -> query
        return {f: f"explain (analyze, timing, format json, verbose, buffers, settings) {open(os.path.join(folder, f), 'r').read()}" for f in files}

    def _get_test_run_id(self, conn):
        cursor = conn.cursor()
        cursor.execute("SELECT nextval('benchmark_results_id_seq');")
        return cursor.fetchone()[0]

    def _retry_get_connection(self):
        while True:
            try:
                conn = get_connection(self._config)
                conn.set_session(autocommit=True)
                return conn
            except Exception as e:
                print(f'Exception thrown while connecting to database, trying again in 5 seconds: {e}')
                time.sleep(5)
                continue
