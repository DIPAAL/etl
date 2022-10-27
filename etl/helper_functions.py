from datetime import datetime, timedelta
from typing import List
from time import perf_counter
import psycopg2


def wrap_with_timings(name: str, func):
    print(f"{name} started at {datetime.now()}")
    start = perf_counter()
    result = func()
    end = perf_counter()
    print(f"{name} finished at {datetime.now()}")
    print(f"{name} took {timedelta(seconds=(end - start))}")
    return result


def get_connection(config, database=None, host=None, user=None, password=None):
    host, port = host.split(
        ':') if host is not None else config['Database']['host'].split(':')
    database = database if database is not None else config['Database']['database']
    user = user if user is not None else config['Database']['user']
    password = password if password is not None else config['Database']['password']
    return psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password,
        port=port
    )


def get_first_query_in_file(file_path: str) -> str:
    queries_list = get_queries_in_file(file_path=file_path)
    return queries_list[0]


def get_queries_in_file(file_path: str) -> List[str]:
    with open(file=file_path, mode='r') as sql_file:
        content = sql_file.read()
        queries = content.split(';')
        queries = [query.strip() for query in queries if query.strip() != '']

        return queries


def apply_datetime_if_not_none(str_in, timestamp_format):
    try:
        d = datetime.strptime(str_in, timestamp_format)
    except Exception:
        d = None
    return d
