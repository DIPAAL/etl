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
        Ensures the existence of ship junk data in the ship junk dimension.

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

        query = """
            INSERT INTO dim_ship_junk (location_system_type, mobile_type, ship_type)
            VALUES {}
            ON CONFLICT (location_system_type, mobile_type, ship_type)
                DO UPDATE SET ship_type = EXCLUDED.ship_type
            RETURNING ship_junk_id
        """

        ship_junks[T_SHIP_JUNK_ID_COL] = self._bulk_insert(ship_junks, conn, query)

        return df.merge(ship_junks, on=unique_columns, how='left')
