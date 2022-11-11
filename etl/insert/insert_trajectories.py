import pandas as pd
from etl.constants import T_SHIP_ID_COL, T_SHIP_TRAJECTORY_ID_COL, T_SHIP_JUNK_ID_COL, \
    T_SHIP_NAVIGATIONAL_STATUS_ID_COL, T_START_DATE_COL, T_START_TIME_COL, T_END_DATE_COL, T_END_TIME_COL, \
    T_ETA_DATE_COL, T_ETA_TIME_COL, T_DURATION_COL, T_LENGTH_COL, T_INFER_STOPPED_COL
from etl.helper_functions import get_connection
from etl.insert.bulk_inserter import BulkInserter
from etl.insert.dimensions.navigational_status_dimension import NavigationalStatusDimensionInserter
from etl.insert.dimensions.ship_dimension import ShipDimensionInserter
from etl.insert.dimensions.ship_junk_dimension import ShipJunkDimensionInserter
from etl.insert.dimensions.trajectory_dimension import TrajectoryDimensionInserter


class TrajectoryInserter (BulkInserter):
    """
    Class responsible for bulk inserting trajectories in a database.
    Inherits from the BulkInserter class.

    Methods
    -------
    persist(df, config): persist trajectory data into a database
    """

    def persist(self, df: pd.DataFrame, config):
        """
        Persist trajectory data into a database.

        Keyword arguments:
            df: dataframe containing trajectory to insert
            config: the application configuration
        """
        # rebuild index to be able to loop over it.
        df = df.reset_index()

        conn = get_connection(config)

        df = ShipDimensionInserter(self.bulk_size).ensure(df, conn)
        df = ShipJunkDimensionInserter(self.bulk_size).ensure(df, conn)
        df = NavigationalStatusDimensionInserter(self.bulk_size).ensure(df, conn)
        df = TrajectoryDimensionInserter(self.bulk_size).ensure(df, conn)

        self._insert_trajectories(df, conn)

        conn.commit()

    def _insert_trajectories(self, df: pd.DataFrame, conn):
        """
        Insert trajectories into database.

        Keyword arguments:
            df: dataframe containing trajectory data
            conn: database connection
        """

        query = """
            INSERT INTO fact_trajectory (
                ship_id, trajectory_id, ship_junk_id, nav_status_id,
                start_date_id, start_time_id, end_date_id, end_time_id,
                eta_date_id, eta_time_id,
                duration, length, infer_stopped
            )
            VALUES {}
        """

        columns = [
            T_SHIP_ID_COL,
            T_SHIP_TRAJECTORY_ID_COL,
            T_SHIP_JUNK_ID_COL,
            T_SHIP_NAVIGATIONAL_STATUS_ID_COL,
            T_START_DATE_COL,
            T_START_TIME_COL,
            T_END_DATE_COL,
            T_END_TIME_COL,
            T_ETA_DATE_COL,
            T_ETA_TIME_COL,
            T_DURATION_COL,
            T_LENGTH_COL,
            T_INFER_STOPPED_COL
        ]

        self._bulk_insert(df[columns], conn, query, fetch=False)
