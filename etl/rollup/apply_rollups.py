from datetime import datetime

from etl.helper_functions import wrap_with_timings


def apply_rollups(conn, date: datetime) -> None:
    wrap_with_timings("Applying cell fact rollup", lambda: apply_cell_fact_rollup(conn, date))


def apply_cell_fact_rollup(conn, date: datetime) -> None:
    # Load the fact_cell_rollup.sql file
    with open('fact_cell_rollup.sql', 'r') as f:
        query = f.read()

    conn.execute(query, [date])


