import pandas as pd

from etl.constants import T_TRAJECTORY_COL, T_ROT_COL, T_HEADING_COL, T_DRAUGHT_COL, T_DESTINATION_COL, \
    T_SHIP_TRAJECTORY_ID_COL
from etl.insert.bulk_inserter import BulkInserter


class TrajectoryDimensionInserter(BulkInserter):
    def ensure(self, df: pd.DataFrame, conn) -> pd.DataFrame:
        trajectories = df[[
            T_TRAJECTORY_COL,
            T_ROT_COL,
            T_HEADING_COL,
            T_DRAUGHT_COL,
            T_DESTINATION_COL
        ]]

        query = """
            INSERT INTO dim_trajectory (
                trajectory,
                rot,
                heading,
                draught,
                destination
            )
            VALUES {}
            RETURNING trajectory_id
        """

        df[T_SHIP_TRAJECTORY_ID_COL] = self._bulk_insert(trajectories, conn, query)
        return df
