"""Module to apply rollups after inserting."""
import os
from datetime import datetime

from etl.helper_functions import wrap_with_timings, measure_time, execute_insert_query_on_connection, \
    extract_smart_date_id_from_date, get_staging_cell_sizes
from etl.audit.logger import global_audit_logger as gal, TIMINGS_KEY, ROWS_KEY


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

    wrap_with_timings("Perform cell fact rollups", lambda: apply_cell_fact_rollups(conn, date))
    wrap_with_timings('Pre-aggregating heatmaps', lambda: apply_heatmap_aggregations(conn, date))


def apply_simplify_query(conn, date: datetime) -> None:
    """
    Apply the simplify query for the given date.

    Args:
        conn: The database connection
        date: The date to apply the rollup for
    """
    with open('etl/rollup/sql/simplify_trajectories.sql', 'r') as f:
        query = f.read()

    date_smart_key = extract_smart_date_id_from_date(date)
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

    date_smart_key = extract_smart_date_id_from_date(date)
    with conn.cursor() as cursor:
        cursor.execute(query, (date_smart_key,))


def apply_heatmap_aggregations(conn, date: datetime) -> None:
    """
    Pre-aggregate heatmaps.

    Keyword Arguments:
        conn: The database connection
        date: The date to pre-aggregate heatmaps for
    """





    date_smart_key = extract_smart_date_id_from_date(date)
    staging_cell_sizes = get_staging_cell_sizes()

    files = os.listdir('etl/rollup/sql/heatmaps')
    files.sort()

    for file in files:
        with open(f'etl/rollup/sql/heatmaps/{file}', 'r') as f:
            query_template = f.read()
        for size in staging_cell_sizes:
            query = query_template.format(CELL_SIZE=size)
            wrap_with_timings(
                f'Creating {file} heatmap for {size}m cells',
                lambda: _apply_heatmap_aggregation(conn,
                                                   date_smart_key,
                                                   query,
                                                   cell_size=size,
                                                   temporal_resolution=84600,
                                                   spatial_resolution=size)
            )


def _apply_heatmap_aggregation(conn, date_key: int, query: str, cell_size: int, temporal_resolution: int,
                               spatial_resolution: int) -> None:
    """
    Pre-aggregate single heatmap.

    Keyword Arguments:
        conn: The database connection
        date_key: The DW smart key for the date to apply aggregation
        query: The aggregation query
        cell_size: The size of the cells the heatmap is created from
        temporal_resolution: The temporal duration in seconds the heatmap spans
        spatial_resolution: The spatial extend in units of the SRID per pixel in the heatmap
    """
    (rows, seconds_elapsed) = measure_time(
        lambda: execute_insert_query_on_connection(conn, query,
                                                   {'DATE_KEY': date_key,
                                                    'TEMPORAL_RESOLUTION': temporal_resolution,
                                                    'SPATIAL_RESOLUTION': spatial_resolution}))

    # Audit log the information
    gal[TIMINGS_KEY][f'fact_cell_heatmap_{cell_size}m_aggregation'] = seconds_elapsed
    gal[ROWS_KEY][f'fact_cell_heatmap_{cell_size}m_aggregation'] = rows


def apply_cell_fact_rollups(conn, date: datetime) -> None:
    """
    Apply the cell fact rollups for the given date. Includes the lazy loading of cell dimensions.

    Args:
        conn: The database connection
        date: The date to apply the rollup for
    """
    with open('etl/rollup/sql/staging_split_trajectories.sql', 'r') as f:
        query = f.read()

    date_smart_key = extract_smart_date_id_from_date(date)

    (rows, seconds_elapsed) = measure_time(
        lambda: execute_insert_query_on_connection(conn, query, (date_smart_key,))
    )
    gal[TIMINGS_KEY]["traj_split_5k"] = seconds_elapsed
    gal[ROWS_KEY]["traj_split_5k"] = rows

    staging_cell_sizes = get_staging_cell_sizes()
    for (cell_size, parent_cell_size) in \
            reversed([*zip(staging_cell_sizes, staging_cell_sizes[1:]), (staging_cell_sizes[-1], None)]):
        wrap_with_timings(
            f"Applying {cell_size}m cell fact rollup",
            lambda: apply_cell_fact_rollup(conn, date, cell_size, parent_cell_size)
        )


def apply_cell_fact_rollup(conn, date: datetime, cell_size: int, parent_cell_size: int) -> None:
    """
    Apply the cell fact rollup and lazy load for the given data and cell size.

    Args:
        conn: The database connection
        date: The date to apply the rollup for
        cell_size: The cell size to apply the rollup for
        parent_cell_size: The parent cell size to apply the lazy load for
    """
    with open('etl/rollup/sql/fact_cell_rollup.sql', 'r') as f:
        cell_fact_rollup_query = f.read()

    cell_fact_rollup_query = cell_fact_rollup_query.format(CELL_SIZE=cell_size)

    date_smart_key = extract_smart_date_id_from_date(date)

    (rows, seconds_elapsed) = measure_time(
        lambda: execute_insert_query_on_connection(conn, cell_fact_rollup_query, (date_smart_key,))
    )
    gal[TIMINGS_KEY][f"fact_cell_{cell_size}m_rollup"] = seconds_elapsed
    gal[ROWS_KEY][f"fact_cell_{cell_size}m_rollup"] = rows

    # We need to commit as we have performed a distributed query, and now need to insert into a reference table.
    conn.commit()

    lazy_load_dim_cell(cell_size, conn, parent_cell_size, date_smart_key)


def lazy_load_dim_cell(cell_size: int, conn, parent_cell_size: int, date_smart_key: int):
    """
    Lazy load the dim_cell table for the given cell size.

    Args:
        cell_size: The cell size to lazy load for
        conn: The database connection
        parent_cell_size: The parent cell size to lazy load for
        date_smart_key: The date smart key to lazy load for
    """
    with open('etl/rollup/sql/lazy_load_cells_from_cell_facts.sql', 'r') as f:
        lazy_dim_cell_query = f.read()
    parent_formula_x = f"cell_x/{(int)(parent_cell_size / cell_size)}" if parent_cell_size else "NULL"
    parent_formula_y = f"cell_y/{(int)(parent_cell_size / cell_size)}" if parent_cell_size else "NULL"
    lazy_dim_cell_query = lazy_dim_cell_query.format(
        CELL_SIZE=cell_size, PARENT_FORMULA_X=parent_formula_x, PARENT_FORMULA_Y=parent_formula_y
    )
    (rows, seconds_elapsed) = measure_time(
        lambda: execute_insert_query_on_connection(conn, lazy_dim_cell_query, (date_smart_key,), fetch_count=True),
    )
    gal[TIMINGS_KEY][f"dim_cell_{cell_size}m_lazy"] = seconds_elapsed
    gal[ROWS_KEY][f"dim_cell_{cell_size}m_lazy"] = rows
    # We need to commit as we have performed a distributed query, and now need to insert into a reference table.
    conn.commit()
