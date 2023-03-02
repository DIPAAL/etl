"""Module to facilitate running sql files."""
import os

from etl.helper_functions import wrap_with_timings, get_connection
from typing import Dict


def get_sql_files(folder):
    """Get all sql files in a folder."""
    files = [f for f in os.listdir(folder) if f.endswith('.sql')]
    # sort alphabetically
    files.sort()
    files = [os.path.join(folder, f) for f in files]
    return files


def run_sql_file_with_timings(sql_file, config, conn=None, format: Dict = None):
    """
    Run a single sql file with timings for every statement.

    Args:
        sql_file: The sql file to run
        config: The config to use
        conn: The connection to use. If None, a new connection will be created
        format: Optional formatting information for the query
    """
    conn = get_connection(config) if conn is None else conn
    conn.set_session(autocommit=True)

    file_contents = open(sql_file, 'r').read()
    queries = file_contents.split(';')
    queries = [q.strip() for q in queries if q.strip() != '']
    for query in queries:
        if format:
            query = query.format(**format)
        # Replace query new lines with spaces, and max 40 characters
        query_short = query.replace('\n', ' ')
        query_short = query_short[:40] + '...' if len(query_short) > 40 else query_short
        wrap_with_timings(f"Executing query: {query_short}", lambda: conn.cursor().execute(query))


def run_sql_folder_with_timings(folder: str, config, conn=None, format: Dict = None) -> None:
    """
    Run all sql files in a folder.

    Args:
        folder: The folder to run the sql files in
        config: The config to use
        conn: The connection to use. If None, a new connection will be created
        format: Optional formatting information for the query
    """
    conn = get_connection(config) if conn is None else conn

    for sql_file in get_sql_files(folder):
        run_sql_file_with_timings(sql_file, config, conn, format)


def run_single_statement_sql_files_in_folder(folder, config, conn=None):
    """
    Run all sql files in a folder, but without splitting statements by ;.

    Args:
        folder: The folder to run the sql files in
        config: The config to use
        conn: The connection to use. If None, a new connection will be created
    """
    conn = get_connection(config) if conn is None else conn

    for sql_file in get_sql_files(folder):
        run_sql_file_with_timings_no_split(sql_file, config, conn)


def run_sql_file_with_timings_no_split(sql_file, config, conn=None):
    """
    Run a single sql file with timings for every statement, but without splitting by ;.

    Args:
        sql_file: The sql file to run
        config: The config to use
        conn: The connection to use. If None, a new connection will be created
    """
    conn.set_session(autocommit=True)
    conn = get_connection(config) if conn is None else conn

    file_contents = open(sql_file, 'r').read()
    # Replace query new lines with spaces, and max 40 characters
    query_short = file_contents.replace('\n', ' ')
    query_short = query_short[:40] + '...' if len(query_short) > 40 else query_short
    wrap_with_timings(f"Executing query: {query_short}", lambda: conn.cursor().execute(file_contents))
