"""Responsible for ensuring the ship dimension."""
import pandas as pd

from etl.constants import T_MMSI_COL, T_IMO_COL, T_SHIP_NAME_COL, T_SHIP_CALLSIGN_COL, T_A_COL, T_B_COL, T_C_COL, \
    T_D_COL, T_SHIP_TYPE_ID_COL, T_LOCATION_SYSTEM_TYPE_COL, MID_COL, UNKNOWN_STRING_VALUE, T_WIDTH_COL, T_LENGTH_COL
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
            T_A_COL, T_B_COL, T_C_COL, T_D_COL, T_LENGTH_COL, T_WIDTH_COL, T_SHIP_TYPE_ID_COL,
            T_LOCATION_SYSTEM_TYPE_COL,
        ]

        ships = df[unique_columns].drop_duplicates()

        # Add the 'mid', 'flag_region', and 'flag_state' contextual attributes to the ship dataframe
        map_query = """
            SELECT *
            FROM staging.mid_map
        """
        mid_map_df = pd.read_sql_query(map_query, conn)

        # Extract mid from mmsi
        ships[MID_COL] = ships.apply(
            func=lambda row: int(str(row[T_MMSI_COL])[:3]),
            axis='columns'
        )

        ships = ships.merge(mid_map_df, on=[MID_COL], how='left') \
            .fillna({'flag_region': UNKNOWN_STRING_VALUE, 'flag_state': UNKNOWN_STRING_VALUE})

        insert_query = """
            INSERT INTO dim_ship (mmsi, imo, name, callsign, a, b, c, d, length, width,
            ship_type_id, location_system_type, mid, flag_region, flag_state)
            VALUES {}
            RETURNING ship_id, mmsi, imo, callsign ship_callsign, name ship_name, a, b, c, d, length, width,
                ship_type_id, location_system_type, mid, flag_region, flag_state
        """

        select_query = """
            SELECT
                ship_id, mmsi, imo, name ship_name, callsign ship_callsign, a, b, c, d, length, width,
                ship_type_id, location_system_type, mid, flag_region, flag_state
            FROM dim_ship
            WHERE (mmsi, imo, name, callsign, a, b, c, d, length, width, ship_type_id, location_system_type, mid,
                flag_region, flag_state) IN {}
            """

        ships = self._bulk_select_insert(ships, conn, insert_query, select_query)

        return df.merge(ships, on=unique_columns, how='left')
