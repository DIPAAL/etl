from typing import List
from trajectory import Trajectory
import geopandas as gpd



def build_from_geopandas(clean_sorted_ais: gpd.GeoDataFrame) -> List[Trajectory]:
    grouped_data = clean_sorted_ais.groupby(by='MMSI')


def _create_trajectory(mmsi: int, data)