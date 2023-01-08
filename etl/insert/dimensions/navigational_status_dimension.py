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

        insert_query = """
            INSERT INTO dim_nav_status (nav_status)
            VALUES {}
            RETURNING nav_status_id
        """

        select_query = """
            SELECT
                nav_status_id, nav_status
            FROM dim_nav_status
            WHERE (nav_status) IN {}
        """

        nav_statuses = self._bulk_select_insert(nav_statuses, conn, insert_query, select_query)

        nav_statuses.rename(columns={'nav_status_id': T_SHIP_NAVIGATIONAL_STATUS_ID_COL}, inplace=True)

        return df.merge(nav_statuses, on=unique_columns, how='left')
