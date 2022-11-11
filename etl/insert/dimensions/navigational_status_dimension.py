"""Responsible for ensuring the navigational status dimension."""
import pandas as pd

from etl.constants import T_NAVIGATIONAL_STATUS_COL, T_SHIP_NAVIGATIONAL_STATUS_ID_COL
from etl.insert.bulk_inserter import BulkInserter


class NavigationalStatusDimensionInserter (BulkInserter):
    """
    Class responsible for bulk inserting navigational status dimension data into a database.

    Inherits from the BulkInserter class.

    Methods
    -------
    ensure(df, conn): ensures the existence of a navigational status in the navigational status dimension
    """

    def ensure(self, df: pd.DataFrame, conn) -> pd.DataFrame:
        """
        Ensure the existence of a navigation status in the navigational status dimension.

        Keyword arguments:
            df: dataframe containing navigational status information
            conn: database connection used for insertion
        """
        unique_columns = [
            T_NAVIGATIONAL_STATUS_COL
        ]

        nav_statuses = df[unique_columns].drop_duplicates()

        query = """
            INSERT INTO dim_nav_status (nav_status)
            VALUES {}
            ON CONFLICT (nav_status)
                DO UPDATE SET nav_status = EXCLUDED.nav_status
            RETURNING nav_status_id
        """

        nav_statuses[T_SHIP_NAVIGATIONAL_STATUS_ID_COL] = self._bulk_insert(nav_statuses, conn, query)

        return df.merge(nav_statuses, on=unique_columns, how='left')
