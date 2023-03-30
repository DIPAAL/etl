"""Restribe columnar tables at the end of the month to get the largest stribes."""
from datetime import datetime
import calendar
from etl.constants import COLUMNAR_TABLE_NAMES, ACCESS_METHOD_COLUMNAR, ACCESS_METHOD_HEAP
from etl.helper_functions import wrap_with_timings, measure_time, get_config
from etl.init.sqlrunner import run_sql_file_with_timings
from etl.audit.logger import global_audit_logger as gal, TIMINGS_KEY


def check_restribe(cur_date: datetime, conn) -> None:
    """
    Restribe columnar tables if currently loaded day is last of month.

    Keyword Arguments:
        cur_date: The date that is currently being loaded
        conn: The database connection
    """
    if not _is_last_day_of_month(cur_date):
        print(f'{cur_date} is not last day of month, no restribing will be done')
        return

    print(f'{cur_date} is last day of month, restribing will commence')

    for table in COLUMNAR_TABLE_NAMES:
        _, elapsed_seconds = wrap_with_timings(
            f'Restribe columnar table: {table}',
            lambda: measure_time(lambda: restribe(table, cur_date, conn)))
        gal[TIMINGS_KEY][f'Restribing {table}'] = elapsed_seconds


def _is_last_day_of_month(cur_date: datetime) -> bool:
    """
    Check if the current date is the last day of the current month.

    Keyword Arguments:
        cur_date: The date that is currently being loaded
    """
    _, num_days_month = calendar.monthrange(cur_date.year, cur_date.month)
    return cur_date.day == num_days_month


def restribe(table_name: str, cur_date: datetime, conn):
    """
    Perform restribing of a given columnar table.

    Keyword Arguments:
        table_name: The name of the columnar table to restribe
        cur_date: The date that is currently being loaded
        conn: The database connection
    """
    partition_name = _get_partition_name(table_name, cur_date)
    config = get_config()
    # Convert to row-major
    run_sql_file_with_timings('etl/insert/sql/set_access_method.sql',
                              config,
                              conn,
                              format=dict(
                                TABLE_NAME=partition_name,
                                ACCESS_METHOD=ACCESS_METHOD_HEAP),
                              set_autocommit=False)
    # Convert to Citus columnar (now restribed)
    run_sql_file_with_timings('etl/insert/sql/set_access_method.sql',
                              config,
                              conn,
                              format=dict(
                                TABLE_NAME=partition_name,
                                ACCESS_METHOD=ACCESS_METHOD_COLUMNAR),
                              set_autocommit=False)


def _get_partition_name(table_name: str, cur_date: datetime) -> str:
    """
    Get the partition name of a table given a date.

    Keyword Arguments:
        table_name: The name of the table to find the partition name
        cur_date: The date currently being loaded
    """
    return f'{table_name}_{cur_date.year}_{str(cur_date.month).zfill(2)}'
