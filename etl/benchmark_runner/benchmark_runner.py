import json
import os
import random
from time import perf_counter

from typing import List

from etl.helper_functions import get_connection, wrap_with_timings


class BenchmarkRunner:

    def __init__(self, config, garbage_queries_between = 10, iterations = 10):
        self.config = config
        self.garbage_queries_between = garbage_queries_between
        self.iterations = iterations
        self.conn = get_connection(config)
        self.conn.set_session(autocommit=True)
        self.garbage_queries = self.get_garbage_queries()
        self.test_run_id = self.get_test_run_id(self.conn)
        print(f"Test run id: {self.test_run_id}")

    def run_benchmark(self):
        # Enable explaining all tasks if not already set.
        query = "SET citus.explain_all_tasks = 1;"
        self.conn.cursor().execute(query)

        # get queries to benchmark
        queries = self.get_queries_to_benchmark()
        # sort by key ascending
        queries = {k: queries[k] for k in sorted(queries)}

        for query_name, query in queries.items():
            for i in range(self.iterations):
                self.run_random_garbage_queries()

                # prepend the query with "explain analyze timings format json "
                query = f"explain (analyze, timing, format json, verbose, buffers, settings) \n{query}"

                cursor = self.conn.cursor()

                start = perf_counter()
                wrap_with_timings(f"Running query {query_name} iteration {i}", lambda: cursor.execute(query))
                end = perf_counter()
                time_taken_ms = int((end - start) * 1000)

                result = cursor.fetchone()[0][0]
                # encode dict to json
                result = json.dumps(result)

                query = """
                    INSERT INTO benchmark_results (test_run_id, query_name, iteration, explain, execution_time_ms)
                    VALUES (%s, %s, %s, %s, %s)                    
                """
                self.conn.cursor().execute(query, (self.test_run_id, query_name, i, result, time_taken_ms))




    def run_random_garbage_queries(self):
        for i in range(self.garbage_queries_between):
            # pick random garbage query
            garbage_query = random.choice(self.garbage_queries)
            wrap_with_timings(f"Garbage Query {i}", lambda: self.conn.cursor().execute(garbage_query))

    def get_queries_to_benchmark(self):
        folder = 'benchmarks/queries'
        return self.__get_queries_in_folder(folder)

    def get_garbage_queries(self) -> List[str]:
        folder = 'benchmarks/garbage_queries'
        return list(self.__get_queries_in_folder(folder).values())

    def __get_queries_in_folder(self, folder):
        files = [f for f in os.listdir(folder) if f.endswith('.sql')]

        # return contents as dict filename -> query
        return {f: open(os.path.join(folder, f), 'r').read() for f in files}

    def get_test_run_id(self, conn):
        cursor = conn.cursor()
        cursor.execute("SELECT nextval('benchmark_results_id_seq');")
        return cursor.fetchone()[0]


if __name__ == '__main__':
    BenchmarkRunner(
        None,
        garbage_queries_between=0,
        iterations=1
    ).run_benchmark()