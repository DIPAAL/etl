from datetime import datetime

from etl.helper_functions import wrap_with_timings


def apply_rollups(conn, date: datetime) -> None:
    """
    Uses the open database connection to apply rollups for the given date
    :param conn: The database connection.
    :param date: The given data
    :return:
    """
    wrap_with_timings("Applying cell fact rollup", lambda: apply_cell_fact_rollup(conn, date))


def apply_cell_fact_rollup(conn, date: datetime) -> None:
    """
    Applies the cell fact rollup for the given date
    :param conn: The database connection
    :param date: The given date
    :return:
    """
    with open('fact_cell_rollup.sql', 'r') as f:
        query = f.read()

    conn.execute(query, [date])


