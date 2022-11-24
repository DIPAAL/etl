"""Responsible for ensuring the ship dimension."""
import pandas as pd

from etl.constants import T_MMSI_COL, T_IMO_COL, T_SHIP_NAME_COL, T_SHIP_CALLSIGN_COL, T_A_COL, T_B_COL, T_C_COL, \
    T_D_COL
from etl.insert.bulk_inserter import BulkInserter


class ShipDimensionInserter (BulkInserter):
    """
    Class responsible for bulk inserting ship dimension data into a database.

    Inherits from the BulkInserter class.

    Methods
    -------
    ensure(df, conn): ensures the existence of a ship in the ship dimension
    """

    def ensure(self, df: pd.DataFrame, conn) -> pd.DataFrame:
        """
        Ensure the existence of a ship in the ship dimension.

        Keyword arguments:
            df: dataframe containing ship dimension data
            conn: database connection used for insertion
        """
        unique_columns = [
            T_MMSI_COL, T_IMO_COL, T_SHIP_NAME_COL, T_SHIP_CALLSIGN_COL,
            T_A_COL, T_B_COL, T_C_COL, T_D_COL
        ]

        ships = df[unique_columns].drop_duplicates()

        insert_query = """
            INSERT INTO dim_ship (mmsi, imo, name, callsign, a, b, c, d)
            VALUES {}
            RETURNING ship_id
        """

        select_query = """
            SELECT
                ship_id, mmsi, imo, name ship_name, callsign ship_callsign, a, b, c, d
            FROM dim_ship
            WHERE (mmsi, imo, name, callsign, a, b, c, d) IN {}
            """

        ships = self._bulk_select_insert(ships, conn, insert_query, select_query)

        return df.merge(ships, on=unique_columns, how='left')
