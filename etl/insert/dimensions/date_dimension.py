"""Responsible for ensuring the ship dimension."""
import pandas as pd

from etl.constants import T_ETA_DATE_COL, T_START_DATE_COL, T_END_DATE_COL


class DateDimensionInserter:
    """
    Class responsible for ensuring the existence of dates in the date dimension.

    Methods
    -------
    ensure(df, conn): ensure the existence of dates in the date dimension
    """

    def ensure(self, df: pd.DataFrame, conn) -> pd.DataFrame:
        """
        Ensure the existence of ETA, start and end dates in the date dimension.

        Keyword arguments:
            df: dataframe containing ship dimension data
            conn: database connection used for insertion
        """
        # collect all unique dates from T_ETA_DATE_COL, T_START_DATE_COL, T_END_DATE_COL
        dates = df[[T_ETA_DATE_COL, T_START_DATE_COL, T_END_DATE_COL]].stack().unique()

        # remove entries of -1
        dates = dates[dates != -1]

        # to array
        dates = dates.tolist()

        query = """
            INSERT INTO dim_date (
                date_id, date, day_of_week, day_of_month,
                day_of_year, week_of_year, month_of_year, quarter_of_year, year
            )
            SELECT
                -- create smart id such that 2022-09-01 gets id 20220901
                (EXTRACT(YEAR FROM date) * 10000) + (EXTRACT(MONTH FROM date) * 100) + (EXTRACT(DAY FROM date))
                    AS date_id,
                date,
                EXTRACT(DOW FROM date),
                EXTRACT(DAY FROM date),
                EXTRACT(DOY FROM date),
                EXTRACT(WEEK FROM date),
                EXTRACT(MONTH FROM date),
                EXTRACT(QUARTER FROM date),
                EXTRACT(YEAR FROM date)
            FROM (SELECT TO_DATE(unnest(%(dates)s)::text, 'YYYYMMDD') AS date) sq
            ON CONFLICT DO NOTHING
        """

        with conn.cursor() as cursor:
            cursor.execute(query, dict(dates=dates))
            conn.commit()
