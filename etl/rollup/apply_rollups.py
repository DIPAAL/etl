"""Module to apply rollups after inserting."""
from datetime import datetime

from etl.helper_functions import wrap_with_timings
from etl.trajectory.builder import extract_date_smart_id
from etl.audit.logger import global_audit_logger as gal
from etl.constants import CELL_SIZES


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

    wrap_with_timings("Applying cell fact rollup", lambda: apply_cell_fact_rollup(conn, date))
    wrap_with_timings('Pre-aggregating heatmaps', lambda: apply_heatmap_aggregations(conn, date))


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


def apply_heatmap_aggregations(conn, date: datetime) -> None:
    """
    Pre-aggregate heatmaps.

    Keyword Arguments:
        conn: The database connection
        date: The date to pre-aggregate heatmaps for
    """
    with open('etl/rollup/sql/heatmap.sql', 'r') as f:
        query_template = f.read()

    date_smart_key = extract_date_smart_id(date)
    for size in CELL_SIZES:
        query = query_template.format(CELL_SIZE=size)
        _apply_heatmap_aggregation(conn, date_smart_key, query, size)


def _apply_heatmap_aggregation(conn, date_key: int, query: str) -> None:
    """
    Pre-aggregate single heatmap.

    Keyword Arguments:
        conn: The database connection
        date_key: The DW smart key for the date to apply aggregation
        query: The aggregation query
    """
    with conn.cursor() as cursor:
        cursor.execute(query, {'date_key': date_key})
