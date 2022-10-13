import pandas as pd
import geopandas as gpd
from etl.helper_functions import get_queries_from_sql_file
from sqlalchemy import create_engine

CSV_EXTENSION = '.csv'
COORDINATE_REFERENCE_SYSTEM = 'epsg:4326'
GEOMETRY_BOUNDS_QUERY = './etl/cleaning/sql/geometry_bounds.sql'

def clean_data(config, ais_file_path: str) -> gpd.GeoDataFrame:
    if ais_file_path.endswith(CSV_EXTENSION):
        return _clean_csv_data(config, ais_file_path)


def _clean_csv_data(config, ais_file_path_csv: str) -> gpd.GeoDataFrame:
    # Read into pandas
    # Keep dictionary of known entities (mimicking staging DB)
    # Coarse cleaning
    #   - Remove where draught >= 28.5 (keep nulls/none)
    #   - Remove where width >= 75
    #   - Remove where length >= 488
    #   - Remove where 99999999 =< MMSI >= 990000000
    #   - Remove where 112000000 < MMSI > 111000000
    #   - Remove where not with geometri of Danish_waters
    
    # Use Geopandas and psycopg2 to get the Danish Waters geometry from the DB.
    # Then uses that to filter on latitude and longitude
    danish_waters_gdf = _get_danish_waters_boundary(config)

    # Read from georeferenced AIS dataframe from csv file
    dirty_geo_dataframe = _create_dirty_df_from_ais_cvs(csv_path=ais_file_path_csv, crs=COORDINATE_REFERENCE_SYSTEM)

    # Initial cleaning of AIS dataframe
    initial_cleaned_dataframe = _ais_df_initial_cleaning(dirty_dataframe=dirty_geo_dataframe)

    # Do a spatial join (inner join) to find all the ships that is within the boundary of danish_waters 
    clean_gdf = gpd.sjoin(initial_cleaned_dataframe, danish_waters_gdf)
    print('Number of rows in boundary cleaned dataframe: ' + str(len(clean_gdf)))

    return clean_gdf



def _get_danish_waters_boundary(config) -> gpd.GeoDataFrame:
    conn = _create_pandas_postgresql_connection(config)

    query_gen = get_queries_from_sql_file(GEOMETRY_BOUNDS_QUERY)
    query = query_gen.__next__()
    return gpd.read_postgis(sql=query, con=conn)

def _create_dirty_df_from_ais_cvs(csv_path: str, crs: str) -> gpd.GeoDataFrame:
    dirty_frame = pd.read_csv(csv_path)
    return gpd.GeoDataFrame(data=dirty_frame, geometry=gpd.points_from_xy(dirty_frame.Longitude, dirty_frame.Latitude), crs=crs)

def _ais_df_initial_cleaning(dirty_dataframe: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    print('Number of rows in dataframe before initial clean: ' + str(len(dirty_dataframe)))
    dirty_dataframe.drop_duplicates()
    dirty_dataframe.query(expr=(
                                '(Draught < 28.5 | Draught.isna()) & '
                                '(Width < 75) & '
                                '(Length < 488) & '
                                '(MMSI < 990000000) & '
                                '(MMSI > 99999999) & '
                                '(MMSI <= 111000000 | MMSI >= 112000000)'
                                ), inplace=True)
    print('Number of rows in dataframe after initial clean: ' + str(len(dirty_dataframe)))
    return dirty_dataframe

def _create_pandas_postgresql_connection(config):
    """
    Creates a connection to the database using SQLalchemy as it is the only connection type supported by pandas
    """
    host, port = config['Database']['host'].split(':')
    connection_url = f"postgresql://{config['Database']['user']}:{config['Database']['password']}@{host}:{port}/{config['Database']['database']}"
    return create_engine(connection_url)