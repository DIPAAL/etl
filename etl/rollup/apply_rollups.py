"""Module to apply rollups after inserting."""
from datetime import datetime

from etl.helper_functions import wrap_with_timings
from etl.trajectory.builder import extract_date_smart_id
from etl.audit.logger import global_audit_logger as gal


def apply_lazy_load_cell_dim(conn, date, cell_size, parent_cell_size):
    """
    Apply the lazy load of the cell dimension.

    Args:
        conn: The database connection
        date: The date to apply the rollup for
        cell_size: The cell size to apply the lazy load for
        cell_dividor: The cell dividor to apply the lazy load for
    """
    with open('etl/rollup/sql/dim_cell_lazy_load.sql', 'r') as f:
        query = f.read()

    query.format(cell_size=cell_size, cell_dividor=parent_cell_size/cell_size)

    date_smart_key = extract_date_smart_id(date)
    with conn.cursor() as cursor:
        cursor.execute(query, (date_smart_key))
        gal.log_etl_stage_rows_cursor("cell_construct", cursor)


def apply_rollups(conn, date: datetime) -> None:
    """
    Use the open database connection to apply rollups for the given date.

    Args:
        conn: The database connection
        date: The date to apply the rollups for
    """

    wrap_with_timings("Applying simplify rollup", lambda: apply_simplify_query(conn, date))
    wrap_with_timings("Applying length calculation rollup", lambda: apply_calc_length_query(conn, date))

    # Commit the changes, this is neccessary as citus does not distribute the rollup query efficiently otherwise.
    conn.commit()

    return

    wrap_with_timings("Applying 50m cell fact rollup", lambda: apply_cell_fact_rollup_initial(conn, date))
    wrap_with_timings("Applying lazy load of 50m cell dim", lambda: apply_lazy_load_cell_dim(conn, date, 50, 200))

    wrap_with_timings("Applying 200m cell fact rollup", lambda: apply_cell_fact_rollup(conn, date, 50, 200))
    wrap_with_timings("Applying lazy load of 200m cell dim", lambda: apply_lazy_load_cell_dim(conn, date, 200, 1000))

    wrap_with_timings("Applying 1000m cell fact rollup", lambda: apply_cell_fact_rollup(conn, date, 200, 1000))
    wrap_with_timings("Applying lazy load of 1000m cell dim", lambda: apply_lazy_load_cell_dim(conn, date, 1000, 5000))

    wrap_with_timings("Applying 5000m cell fact rollup", lambda: apply_cell_fact_rollup(conn, date, 1000, 5000))
    wrap_with_timings("Applying lazy load of 5000m cell dim", lambda: apply_lazy_load_cell_dim(conn, date, 5000, 10000))


def apply_cell_fact_rollup_initial(conn, date: datetime) -> None:
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


def apply_calc_length_query(conn, date: datetime) -> None:
    """
    Apply the length calculation query for the given date.

    Args:
        conn: The database connection
        date: The date to apply the rollup for
    """
    with open('etl/rollup/sql/calc_length.sql', 'r') as f:
        query = f.read()

    date_smart_key = extract_date_smart_id(date)
    with conn.cursor() as cursor:
        cursor.execute(query, (date_smart_key,))
