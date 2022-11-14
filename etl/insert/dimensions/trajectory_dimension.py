"""Responsible for ensuring the trajectory dimension."""
import pandas as pd

from etl.constants import T_TRAJECTORY_COL, T_ROT_COL, T_HEADING_COL, T_DRAUGHT_COL, T_DESTINATION_COL, \
    T_START_DATE_COL, T_TRAJECTORY_SUB_ID_COL
from etl.insert.bulk_inserter import BulkInserter


class TrajectoryDimensionInserter(BulkInserter):
    """
    Class responsible for bulk inserting trajectory dimension data into a database.

    Inherits from the BulkInserter class.

    Methods
    -------
    ensure(df, conn): ensures the existence of a trajectory in the trajectory dimension
    """

    def ensure(self, df: pd.DataFrame, conn) -> pd.DataFrame:
        """
        Ensure the existence of a trajectory in the trajectory dimension.

        Keyword arguments:
            df: dataframe containing trajectory information
            conn: database connection used for insertion
        """

        trajectories = df[[
            T_START_DATE_COL,
            T_TRAJECTORY_SUB_ID_COL,
            T_TRAJECTORY_COL,
            T_ROT_COL,
            T_HEADING_COL,
            T_DRAUGHT_COL,
            T_DESTINATION_COL
        ]]

        query = """
            INSERT INTO dim_trajectory (
                date_id,
                trajectory_sub_id,
                trajectory,
                rot,
                heading,
                draught,
                destination
            )
            VALUES {}
        """
        self._bulk_insert(trajectories, conn, query, fetch=False)
        return df
