"""Module for inserting trajectories in bulk."""
import random

import pandas as pd
from etl.constants import T_SHIP_ID_COL, \
    T_SHIP_NAVIGATIONAL_STATUS_ID_COL, T_START_DATE_COL, T_START_TIME_COL, T_END_DATE_COL, T_END_TIME_COL, \
    T_ETA_DATE_COL, T_ETA_TIME_COL, T_DURATION_COL, T_INFER_STOPPED_COL, T_TRAJECTORY_SUB_ID_COL, INT32_MAX, \
    T_SHIP_TYPE_ID_COL
from etl.helper_functions import get_connection
from etl.insert.bulk_inserter import BulkInserter
from etl.insert.ensure_partitions import ensure_partitions_for_partitioned_tables
from etl.insert.dimensions.date_dimension import DateDimensionInserter
from etl.insert.dimensions.navigational_status_dimension import NavigationalStatusDimensionInserter
from etl.insert.dimensions.ship_dimension import ShipDimensionInserter
from etl.insert.dimensions.trajectory_dimension import TrajectoryDimensionInserter
from etl.insert.dimensions.ship_type_dimension import ShipTypeDimensionInserter


class TrajectoryInserter(BulkInserter):
    """
    Class responsible for bulk inserting trajectories in a database.

    Inherits from the BulkInserter class.

    Methods
    -------
    persist(df, config): persist trajectory data into a database
    """

    @staticmethod
    def generate_unique_random_series(df, max, sampler=random.sample):
        """
        Generate a unique random series with length equal to the length of the dataframe.

        This method can become very computationally heavy if the dataframe length is approaching the max value.

        Args:
            df: dataframe to generate a random series for
            max: maximum value of the random series
            sampler: function used to generate a random series
        """
        initial = pd.Series(sampler(range(max), len(df)))
        # remove duplicates
        initial = initial[~initial.duplicated()]

        # if there were duplicates, make sure to match length of dataframe
        while len(initial) < len(df):
            initial = pd.concat([initial, pd.Series(sampler(range(max), len(df) - len(initial)))])
            initial = initial[~initial.duplicated()]
        return initial

    def persist(self, df: pd.DataFrame, config):
        """
        Persist trajectory data into a database.

        Keyword arguments:
            df: dataframe containing trajectory to insert
            config: the application configuration
        """
        # rebuild index to be able to loop over it.
        df = df.reset_index()
        df[T_TRAJECTORY_SUB_ID_COL] = self.generate_unique_random_series(df, INT32_MAX)

        conn = get_connection(config)

        # Ensure date id and partitions exists
        ensure_partitions_for_partitioned_tables(conn, int(df[T_START_DATE_COL].iloc[0]))

        DateDimensionInserter().ensure(df, conn)
        df = ShipTypeDimensionInserter('dim_ship_type', bulk_size=self.bulk_size,
                                       id_col_name=T_SHIP_TYPE_ID_COL).ensure_with_timings(df, conn)
        df = ShipDimensionInserter("dim_ship", bulk_size=500, id_col_name=T_SHIP_ID_COL).ensure_with_timings(df, conn)
        df = NavigationalStatusDimensionInserter("dim_nav_status", bulk_size=self.bulk_size,
                                                 id_col_name="nav_status_id").ensure_with_timings(df, conn)
        df = TrajectoryDimensionInserter("dim_trajectory", bulk_size=500).ensure_with_timings(df, conn)

        self.ensure_with_timings(df, conn)

        return conn

    def ensure(self, df: pd.DataFrame, conn):
        """
        Insert trajectories into database.

        Keyword arguments:
            df: dataframe containing trajectory data
            conn: database connection
        """
        query = """
            INSERT INTO fact_trajectory (
                ship_id, trajectory_sub_id, nav_status_id,
                start_date_id, start_time_id, end_date_id, end_time_id,
                eta_date_id, eta_time_id,
                duration, infer_stopped
            )
            VALUES {}
        """

        columns = [
            T_SHIP_ID_COL,
            T_TRAJECTORY_SUB_ID_COL,
            T_SHIP_NAVIGATIONAL_STATUS_ID_COL,
            T_START_DATE_COL,
            T_START_TIME_COL,
            T_END_DATE_COL,
            T_END_TIME_COL,
            T_ETA_DATE_COL,
            T_ETA_TIME_COL,
            T_DURATION_COL,
            T_INFER_STOPPED_COL
        ]

        self._bulk_insert(df[columns], conn, query, fetch=False)
