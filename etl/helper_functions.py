"""Helper functions for the ETL process."""
from datetime import datetime, timedelta
from typing import List, Tuple, Callable, TypeVar, Dict
from time import perf_counter
from sqlalchemy import create_engine, Connection, Engine, text

import pandas as pd
import configparser
import os
from etl.audit.logger import global_audit_logger as gal, TIMINGS_KEY
from etl.constants import UNKNOWN_INT_VALUE, SqlalchemyIsolationLevel

ENGINE_DICT: Dict[str, Engine] = {}


def wrap_with_timings(name: str, func, audit_etl_stage: str = None):
    """
    Execute a given function and prints the time it took the function to execute.

    Keyword arguments:
        name: identifier for the function execution, used to identify it in the output
        func: the zero argument function to execute
        audit_etl_stage: name of the ETL stage, must be a valid ETL stage name. If used, the ETL stage will be logged.
            (default: None)

    Examples
    --------
    >>> wrap_with_timings('my awesome addition', lambda: 2+3)
    my awesome addition started at 01/01/2021 00:00:00.00000
    my awesome addition finished at 01/01/2021 00:00:00.00003
    my awesome addition took 0:00:00.00003
    """
    print(f"{name} started at {datetime.now()}")
    start = perf_counter()
    result = func()
    end = perf_counter()
    print(f"{name} finished at {datetime.now()}")
    print(f"{name} took {timedelta(seconds=(end - start))}")

    # Audit logging - Name of the ETL stage and the time it took to execute
    if audit_etl_stage is not None:
        gal[TIMINGS_KEY][audit_etl_stage] = end - start

    return result


# Type variable for the return type of the function passed to measure_time.
# Used to indicate same return type as the function parameter
T = TypeVar('T')


def measure_time(func: Callable[[], T]) -> Tuple[T, float]:
    """
    Execute a given function and return a tuple with the result and the time it took to execute.

    Keyword arguments:
        func: the zero argument function to execute
    """
    start = perf_counter()
    result = func()
    end = perf_counter()
    return result, end - start


def get_connection(config, auto_commit_connection: bool = False, database: str | None = None, host: str | None = None,
                   user: str | None = None, password: str | None = None) -> Connection:
    """
    Return a connection to the database using SQLalchemy, as it is the only connection type supported by pandas.

    Keyword arguments:
        config: the application configuration
        auto_commit_connection: whether the created connection should autocommit
        database: the name of the database (default None)
        host: host and port of the database concatenated using ':' (default None)
        user: username for the database user to use (default None)
        password: password for the database user (defualt None)
    """
    host, port = host.split(':') if host is not None else config['Database']['host'].split(':')
    database = database if database is not None else config['Database']['database']
    user = user if user is not None else config['Database']['user']
    password = password if password is not None else config['Database']['password']
    connection_url = f'postgresql://{user}:{password}@{host}:{port}/{database}'
    if connection_url not in ENGINE_DICT.keys():
        engine = create_engine(connection_url)
        ENGINE_DICT[connection_url] = engine

    connection = ENGINE_DICT[connection_url].connect()
    if auto_commit_connection:
        connection.execution_options(isolation_level=SqlalchemyIsolationLevel.AUTOCOMMIT.value)
    return connection


def get_first_query_in_file(file_path: str) -> str:
    """
    Return the first query found in a given file.

    Keyword arguments:
        file_path: absolute or relative file path to the file containing the sql queries
    """
    queries_list = get_queries_in_file(file_path=file_path)
    return queries_list[0]


def get_queries_in_file(file_path: str) -> List[str]:
    """
    Return the queries found within a file.

    Keywork arguments:
        file_path: absolute or relative file path to the file containing the sql queries
    """
    with open(file=file_path, mode='r') as sql_file:
        content = sql_file.read()
        queries = content.split(';')
        queries = [query.strip() for query in queries if query.strip() != '']

        return queries


def execute_insert_query_on_connection(conn: Connection, query: str, params=None, fetch_count: bool = False) -> int:
    """
    Execute a query on a connection.

    Args:
        conn: The connection to execute the query on
        query: The query to execute
        params: The parameters to pass to the query
        fetch_count: If true, the return value will be using fetch instead of cursor row count.
    """
    result = conn.execute(text(query), params)
    if fetch_count:
        return result.fetchone()[0]
    return result.rowcount


def extract_smart_date_id_from_date(date: datetime) -> int:
    """
    Extract the smart date id from a given date.

    Keyword arguments:
        date: the date to extract the smart date id from
    """
    if pd.isna(date):
        return UNKNOWN_INT_VALUE
    return (date.year * 10000) + (date.month * 100) + date.day


def extract_date_from_smart_date_id(smart_date_id: int) -> datetime:
    """
    Extract the date from a given smart date id.

    Keyword arguments:
        smart_date_id: the smart date id to extract the date from
    """
    return datetime.strptime(str(smart_date_id), '%Y%m%d')


config = None  # Global configuration variable


def get_config():
    """Get the application configuration."""
    global config
    if config is None:
        path = './config.properties'
        local_path = './config-local.properties'
        if os.path.isfile(local_path):
            path = local_path
        config = configparser.ConfigParser()
        config.read(path)

    return config


def get_staging_cell_sizes() -> List[int]:
    """Get the cell sizes to insert into staging area from config."""
    config = get_config()
    return [int(size) for size in config['Database']['staging_cell_sizes_to_insert'].split(',')]


def flatten_string_list(lst: List[str], separator: str = '_') -> str:
    """
    Flatten list of strings to a single string with separated values.

    Arguments:
        lst: list of string values to flatten
        separator: separator used to separate the flattened values (default: '_')
    """
    lst_str = ''
    for elm in lst:
        lst_str += f'{separator}{elm.lower().replace(" ", separator)}'
    return lst_str
