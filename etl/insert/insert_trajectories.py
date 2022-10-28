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

    def insert_trajectory_dataframe(self, df, config):
        conn = get_connection(config)

        df = ShipDimensionInserter().ensure(df, conn)
        df = ShipJunkDimensionInserter().ensure(df, conn)
        df = NavigationalStatusDimensionInserter().ensure(df, conn)
        df = TrajectoryDimensionInserter().ensure(df, conn)

        self._insert_trajectories(df, conn)

        conn.commit()

    def _insert_trajectories(self, df, conn):
        # TODO: hack while length is not available
        df[T_LENGTH_COL] = 0

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
