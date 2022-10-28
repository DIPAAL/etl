import pandas as pd

from etl.constants import T_NAVIGATIONAL_STATUS_COL, T_SHIP_NAVIGATIONAL_STATUS_ID_COL
from etl.insert.bulk_inserter import BulkInserter


class NavigationalStatusDimensionInserter (BulkInserter):
    def ensure(self, df: pd.DataFrame, conn) -> pd.DataFrame:
        unique_columns = [
            T_NAVIGATIONAL_STATUS_COL
        ]

        ship_junks = df[unique_columns].drop_duplicates()

        query = """
            INSERT INTO dim_nav_status (nav_status)
            VALUES {}
            ON CONFLICT (nav_status)
                DO UPDATE SET nav_status = EXCLUDED.nav_status
            RETURNING nav_status_id
        """

        ship_junks[T_SHIP_NAVIGATIONAL_STATUS_ID_COL] = self._bulk_insert(ship_junks, conn, query)

        return df.merge(ship_junks, on=unique_columns, how='left')
