"""Responsible for ensuring the ship dimension."""
import pandas as pd

from etl.constants import T_ETA_DATE_COL, T_START_DATE_COL, T_END_DATE_COL
from sqlalchemy import Connection, text


class DateDimensionInserter:
    """
    Class responsible for ensuring the existence of dates in the date dimension.

    Methods
    -------
    ensure(df, conn): ensure the existence of dates in the date dimension
    """

    def ensure(self, df: pd.DataFrame, conn: Connection) -> pd.DataFrame:
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
                day_of_year, week_of_year, month_of_year, quarter_of_year, year, iso_year,
                day_name, month_name, weekday, season, holiday
            )
            SELECT
                i1.*,
                dm.day_name,
                mm.month_name,
                dm.weekday,
                mm.season,
                (CASE WHEN EXISTS (
                    SELECT 1 FROM (
                        SELECT fh.month, fh.day FROM staging.fixed_holidays fh
                        UNION
                        SELECT eh.month, eh.day FROM calculate_easter_holidays(i1.year::SMALLINT) eh
                    ) AS holidays
                    WHERE holidays.month = i1.month_of_year
                    AND holidays.day = i1.day_of_month)
                THEN 'holiday'
                ELSE 'non-holiday'
                END) AS holiday
            FROM (
                     SELECT
                         -- create smart id such that 2022-09-01 gets id 20220901
                         (EXTRACT(YEAR FROM date) * 10000) + (EXTRACT(MONTH FROM date) * 100) + (EXTRACT(DAY FROM date))
                         AS date_id,
                         date AS date,
                         EXTRACT(DOW FROM date)     AS day_of_week,
                         EXTRACT(DAY FROM date)     AS day_of_month,
                         EXTRACT(DOY FROM date)     AS day_of_year,
                         EXTRACT(WEEK FROM date)    AS week_of_year,
                         EXTRACT(MONTH FROM date)   AS month_of_year,
                         EXTRACT(QUARTER FROM date) AS quarter_of_year,
                         EXTRACT(YEAR FROM date)    AS year,
                         EXTRACT(ISOYEAR FROM date) AS iso_year
                     FROM (SELECT TO_DATE(unnest(%(dates)s)::text, 'YYYYMMDD') AS date) sq
                 ) i1
            INNER JOIN staging.day_num_map dm ON dm.day_num = i1.day_of_week
            INNER JOIN staging.month_num_map mm ON mm.month_num = i1.month_of_year
            ON CONFLICT DO NOTHING
        """

        conn.execute(text(query), dict(dates=dates))
        conn.commit()
