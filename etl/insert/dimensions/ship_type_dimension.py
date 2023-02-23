"""Responsible for ensuring the ship type dimension."""
import pandas as pd

from etl.insert.bulk_inserter import BulkInserter
from etl.constants import T_LOCATION_SYSTEM_TYPE_COL, T_MOBILE_TYPE_COL, T_SHIP_TYPE_COL


class ShipTypeDimensionInserter(BulkInserter):
    """
    Class responsible for bulk inserting ship type dimension data into a database.

    Inherits from the BulkInserter class.

    Methods
    -------
    ensure(df, conn): ensures the existence of a ship type in the ship type dimension
    """

    def ensure(self, df: pd.DataFrame, conn) -> pd.DataFrame:
        """
        Ensure the existence of a ship type in the ship type dimension.

        Keyword arguments:
            df: dataframe containing ship type dimension data
            conn: database connection used for insertion
        """
        unique_columns = [
            T_LOCATION_SYSTEM_TYPE_COL, T_MOBILE_TYPE_COL, T_SHIP_TYPE_COL
        ]

        ship_types = df[unique_columns].drop_duplicates()

        insert_query = """
            INSERT INTO dim_ship_type (location_system_type, mobile_type, ship_type)
            VALUES {}
            RETURNING ship_type_id, location_system_type, mobile_type, ship_type
        """

        select_query = """
            SELECT ship_type_id, location_system_type, mobile_type, ship_type
            FROM dim_ship_type
            WHERE (location_system_type, mobile_type, ship_type) IN {}
        """

        ship_types = self._bulk_select_insert(ship_types, conn, insert_query, select_query)

        return df.merge(ship_types, on=unique_columns, how='left')
