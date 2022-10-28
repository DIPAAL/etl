import pandas as pd

from etl.constants import T_MMSI_COL, T_IMO_COL, T_SHIP_NAME_COL, T_SHIP_CALLSIGN_COL, T_A_COL, T_B_COL, T_C_COL, \
    T_D_COL, T_SHIP_ID_COL
from etl.insert.bulk_inserter import BulkInserter


class ShipDimensionInserter (BulkInserter):
    def ensure(self, df: pd.DataFrame, conn) -> pd.DataFrame:
        unique_columns = [
            T_MMSI_COL, T_IMO_COL, T_SHIP_NAME_COL, T_SHIP_CALLSIGN_COL,
            T_A_COL, T_B_COL, T_C_COL, T_D_COL
        ]

        ships = df[unique_columns].drop_duplicates()

        query = """
            INSERT INTO dim_ship (imo, mmsi, name, callsign, a, b, c, d)
            VALUES {}
            ON CONFLICT (imo, mmsi, name, callsign, a, b, c, d) DO UPDATE SET name = EXCLUDED.name
            RETURNING ship_id
        """

        ships[T_SHIP_ID_COL] = self._bulk_insert(ships, conn, query)

        return df.merge(ships, on=unique_columns, how='left')
