from datetime import datetime, timedelta
from time import perf_counter
from typing import Generator
import psycopg2


def wrap_with_timings(name: str, func) -> None:
    print(f"{name} started at {datetime.now()}")
    start = perf_counter()
    func()
    end = perf_counter()
    print(f"{name} finished at {datetime.now()}")
    print(f"{name} took {timedelta(seconds=(end - start))}")

conn = None
def get_connection(config):
    global conn
    if conn is not None:
        return conn

    host, port = config['Database']['host'].split(':')
    conn = psycopg2.connect(
        host=host,
        database=config['Database']['database'],
        user=config['Database']['user'],
        password=config['Database']['password'],
        port=port
    )
    return conn

def get_queries_from_sql_file(file_path: str) -> Generator[str, None, None]:
    with open(file=file_path, mode='r') as sql_file:
        content = sql_file.read()
        queries = content.split(';')
        queries.remove('')
        for query in queries:
            query = "".join(query.splitlines())
            query + ';'
            yield query
