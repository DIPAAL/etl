"""Module to apply rollups after inserting."""
from datetime import datetime

from etl.helper_functions import wrap_with_timings
from etl.trajectory.builder import extract_date_smart_id
from etl.audit.logger import global_audit_logger as gal


def apply_rollups(conn, date: datetime) -> None:
    """
    Use the open database connection to apply rollups for the given date.

    Args:
        conn: The database connection
        date: The date to apply the rollups for
    """
    wrap_with_timings("Applying simplify rollup", lambda: apply_simplify_query(conn, date))
    # commit for testing
    conn.commit()
    wrap_with_timings("Applying cell fact rollup", lambda: apply_cell_fact_rollup(conn, date))


def apply_cell_fact_rollup(conn, date: datetime) -> None:
    """
    Apply the cell fact rollup for the given date.

    Args:
        conn: The database connection
        date: The date to apply the rollup for
    """
    with open('etl/rollup/sql/fact_cell_rollup.sql', 'r') as f:
        query = f.read()

    date_smart_key = extract_date_smart_id(date)
    with conn.cursor() as cursor:
        cursor.execute(query, (date_smart_key,))
        gal.log_etl_stage_rows_cursor("cell_construct", cursor)


def apply_simplify_query(conn, date: datetime) -> None:
    """
    Apply the simplify query for the given date.

    Args:
        conn: The database connection
        date: The date to apply the rollup for
    """
    with open('etl/rollup/sql/simplify_trajectories.sql', 'r') as f:
        query = f.read()

    date_smart_key = extract_date_smart_id(date)
    with conn.cursor() as cursor:
        cursor.execute(query, (date_smart_key,))
