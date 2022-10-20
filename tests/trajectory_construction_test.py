from etl.trajectory.builder import build_from_geopandas, _rebuild_to_geodataframe
import geopandas as gpd
import pandas as pd

CLEAN_DATA_CSV='tests/data/clean_df.csv'

def create_geopandas_dataframe() -> gpd.GeoDataFrame:
    pandas_df = pd.read_csv(CLEAN_DATA_CSV)
    return _rebuild_to_geodataframe(pandas_dataframe=pandas_df)

def test_builder():
    gdf = create_geopandas_dataframe()
    build_from_geopandas(gdf)
    # Always make it stop, in order to see the prints
