import os

from etl.helper_functions import wrap_with_timings, get_connection


def get_sql_files(folder):
    """Get all sql files in a folder"""
    files = sorted([f for f in os.listdir(folder) if f.endswith('.sql')])
    # sort alphabetically
    files = [os.path.join(folder, f) for f in files]
    return files


def run_sql_file_with_timings(sql_file, config, conn=None):
    conn.set_session(autocommit=True)
    conn = get_connection(config) if conn is None else conn

    file_contents = open(sql_file, 'r').read()
    queries = file_contents.split(';')
    queries = [q.strip() for q in queries if q.strip() != '']
    for query in queries:
        # Replace query new lines with spaces, and max 40 characters
        query_short = query.replace('\n', ' ')
        query_short = query_short[:40] + \
            '...' if len(query_short) > 40 else query_short
        wrap_with_timings(
            f"Executing query: {query_short}",
            lambda: conn.cursor().execute(query))


def run_sql_folder_with_timings(folder: str, config, conn=None) -> None:
    conn = get_connection(config) if conn is None else conn

    for sql_file in get_sql_files(folder):
        run_sql_file_with_timings(sql_file, config, conn)
