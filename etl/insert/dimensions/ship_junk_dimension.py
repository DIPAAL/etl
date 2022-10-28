import pandas as pd

from etl.constants import T_LOCATION_SYSTEM_TYPE_COL, T_MOBILE_TYPE_COL, T_SHIP_TYPE_COL, T_SHIP_JUNK_ID_COL
from etl.insert.bulk_inserter import BulkInserter


class ShipJunkDimensionInserter (BulkInserter):
    def ensure(self, df: pd.DataFrame, conn) -> pd.DataFrame:
        unique_columns = [
            T_LOCATION_SYSTEM_TYPE_COL,
            T_MOBILE_TYPE_COL,
            T_SHIP_TYPE_COL
        ]

        ship_junks = df[unique_columns].drop_duplicates()

        query = """
            INSERT INTO dim_ship_junk (location_system_type, mobile_type, ship_type)
            VALUES {}
            ON CONFLICT (location_system_type, mobile_type, ship_type)
                DO UPDATE SET ship_type = EXCLUDED.ship_type
            RETURNING ship_junk_id
        """

        res = self._bulk_insert(ship_junks, conn, query)
        ship_junks[T_SHIP_JUNK_ID_COL] = res

        return df.merge(ship_junks, on=unique_columns, how='left')
