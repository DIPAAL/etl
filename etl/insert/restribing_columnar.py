from datetime import datetime
import calendar
from etl.constants import COLUMNAR_TABLE_NAMES, ACCESS_METHOD_COLUMNAR, ACCESS_METHOD_HEAP
from etl.helper_functions import wrap_with_timings, get_config
from etl.init.sqlrunner import run_sql_file_with_timings



def check_restribe(cur_date: datetime, conn) -> None:
    """
    Restribe columnar tables if currently loaded day is last of month.
    """
    if not _is_last_day_of_month(cur_date):
        print(f'{cur_date} is not last day of month, no restribing will be done')
        return
    
    print(f'{cur_date} is last day of month, restribing will commence')

    for table in COLUMNAR_TABLE_NAMES:
        wrap_with_timings(f'Restribe columnar table: {table}', lambda: restribe(table, cur_date, conn))


def _is_last_day_of_month(cur_date: datetime) -> bool:
    """
    Check if the current date is the last day of the current month.
    """
    _, num_days_month = calendar.monthrange(cur_date.year, cur_date.month)
    return cur_date.day == num_days_month


def restribe(table_name: str, cur_date: datetime, conn):
    """
    Perform restribing of a given columnar table.
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
    """
    return f'{table_name}_{cur_date.year}_{str(cur_date.month).zfill(2)}'