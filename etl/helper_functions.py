"""Helper functions for the ETL process."""
from datetime import datetime, timedelta
from typing import List
from time import perf_counter
import psycopg2
from etl.audit.logger import global_audit_logger as gal


def wrap_with_timings(name: str, func, audit_etl_stage: str = None):
    """
    Execute a given function and prints the time it took the function to execute.

    Keyword arguments:
        name: identifier for the function execution, used to identify it in the output
        func: the zero argument function to execute
        audit_etl_stage: name of the ETL stage, must be a valid ETL stage name. If used, the ETL stage will be logged.
            (default None)

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
        gal.log_etl_stage_time(audit_etl_stage, start, end)

    return result


def get_connection(config, database=None, host=None, user=None, password=None):
    """
    Return a connection to the database.

    Keyword arguments:
        config: the application configuration
        database: the name of the database (default None)
        host: host and port of the database concatenated using ':' (default None)
        user: username for the database user to use (default None)
        password: password for the database user (defualt None)
    """
    host, port = host.split(':') if host is not None else config['Database']['host'].split(':')
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
