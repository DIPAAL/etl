"""Responsible for ensuring the ship junk dimension."""
import pandas as pd

from etl.constants import T_LOCATION_SYSTEM_TYPE_COL, T_MOBILE_TYPE_COL, T_SHIP_TYPE_COL, T_SHIP_JUNK_ID_COL
from etl.insert.bulk_inserter import BulkInserter


class ShipJunkDimensionInserter (BulkInserter):
    """
    Class responsible for bulk inserting ship junk dimension data into a database.

    Inherits from the BulkInserter class.

    Methods
    -------
    ensure(df, conn): ensures the existence of a ship junk data in the ship junk dimension
    """

    def ensure(self, df: pd.DataFrame, conn) -> pd.DataFrame:
        """
        Ensure the existence of ship junk data in the ship junk dimension.

        Keyword arguments:
            df: dataframe containing ship junk information
            conn: database connection used for insertion
        """
        unique_columns = [
            T_LOCATION_SYSTEM_TYPE_COL,
            T_MOBILE_TYPE_COL,
            T_SHIP_TYPE_COL
        ]

        ship_junks = df[unique_columns].drop_duplicates()

        insert_query = """
            INSERT INTO dim_ship_junk (location_system_type, mobile_type, ship_type)
            VALUES {}
            RETURNING ship_junk_id
        """

        select_query = """
            SELECT 
                ship_junk_id, location_system_type, mobile_type, ship_type
            FROM dim_ship_junk
            WHERE (location_system_type, mobile_type, ship_type) IN {}
        """

        ship_junks = self._bulk_select_insert(ship_junks, conn, insert_query, select_query)

        return df.merge(ship_junks, on=unique_columns, how='left')
