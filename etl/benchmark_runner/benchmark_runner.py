import os
import random

from typing import List

from etl.helper_functions import get_connection, wrap_with_timings


class BenchmarkRunner

    def __init__(self, config, garbage_queries_between = 10, iterations = 10):
        self.config = config
        self.garbage_queries_between = garbage_queries_between
        self.iterations = iterations
        self.conn = get_connection(config)
        self.conn.set_session(autocommit=True)
        self.garbage_queries = self.get_garbage_queries()
        self.test_run_id = random.randint(0, 2**31) #random int32

    def run_benchmark(self):
        # Enable explaining all tasks if not already set.
        query = "SET citus.explain_all_tasks = 1;"
        self.conn.cursor().execute(query)

        # get queries to benchmark
        queries = self.get_queries_to_benchmark()

        for query_name, query in queries.items():
            for i in range(self.iterations):
                self.run_random_garbage_queries()

                # prepend the query with "explain analyze timings format json "
                query = f"explain (analyze, timings, format json, verbose) \n{query}"
                wrap_with_timings(f"Running query {query_name} iteration {i}", lambda: self.conn.cursor().execute(query))

                # get the resulting json
                result = self.conn.cursor().fetchone()[0]

                print(result)
                query = """
                    INSERT INTO benchmark_results (test_run_id, query_name, iteration, result)
                    VALUES (%s, %s, %s, %s)                    
                """
                self.conn.cursor().execute(query, (self.test_run_id, query_name, i, result))




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