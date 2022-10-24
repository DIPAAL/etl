import geopandas as gpd
import dask.dataframe as dd
import dask_geopandas as d_gpd
from etl.helper_functions import wrap_with_timings, get_first_query_in_file
from sqlalchemy import create_engine
from datetime import datetime
from etl.helper_functions import apply_datetime_if_not_none
import multiprocessing

CSV_EXTENSION = '.csv'
COORDINATE_REFERENCE_SYSTEM = 'epsg:4326'
GEOMETRY_BOUNDS_QUERY = './etl/cleaning/sql/geometry_bounds.sql'
NUM_PARTITIONS = 4 * multiprocessing.cpu_count()
CVS_TIMESTAMP_FORMAT='%d/%m/%Y %H:%M:%S' #07/09/2021 00:00:00

def clean_data(config, ais_file_path: str) -> gpd.GeoDataFrame:
    if ais_file_path.endswith(CSV_EXTENSION):
        return _clean_csv_data(config, ais_file_path)
    
    raise NotImplementedError(f'Extension of file provided {ais_file_path}, is not supported in this version of the project.')


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
    danish_waters_gdf = wrap_with_timings('Fetch Danish Waters', lambda: _get_danish_waters_boundary(config))

    # Read from georeferenced AIS dataframe from csv file
    dirty_geo_dataframe = wrap_with_timings(
        'Create Geodataframe from CSV',
        lambda: _create_dirty_df_from_ais_cvs(csv_path=ais_file_path_csv, crs=COORDINATE_REFERENCE_SYSTEM)
    )

    # Initial cleaning of AIS dataframe
    initial_cleaned_dataframe = wrap_with_timings(
        'Initial clean',
        lambda: _ais_df_initial_cleaning(dirty_dataframe=dirty_geo_dataframe)
    )

    # Do a spatial join (inner join) to find all the ships that is within the boundary of danish_waters 
    lazy_clean = d_gpd.sjoin(initial_cleaned_dataframe, danish_waters_gdf, predicate='within')
    clean_gdf = wrap_with_timings('Spatial cleaning', lambda: lazy_clean.compute())
    print('Number of rows in boundary cleaned dataframe: ' + str(len(clean_gdf.index)))

    return clean_gdf



def _get_danish_waters_boundary(config) -> d_gpd.GeoDataFrame:
    conn = _create_pandas_postgresql_connection(config)

    query = get_first_query_in_file(GEOMETRY_BOUNDS_QUERY)
    temp_waters = gpd.read_postgis(sql=query, con=conn)
    return d_gpd.from_geopandas(data=temp_waters, npartitions=1)

def _create_dirty_df_from_ais_cvs(csv_path: str, crs: str) -> d_gpd.GeoDataFrame:
    dirty_frame = dd.read_csv(csv_path, dtype={'Callsign': 'object', 'Cargo type': 'object', 'Destination': 'object', 'ETA': 'object', 'Name': 'object'})
    dirty_frame['timestamp'] = dirty_frame['# Timestamp'].apply(func=lambda t: apply_datetime_if_not_none(t, CVS_TIMESTAMP_FORMAT))
    dirty_frame['ETA'] = dirty_frame['ETA'].apply(func=lambda t: apply_datetime_if_not_none(t, CVS_TIMESTAMP_FORMAT))

    return d_gpd.from_dask_dataframe(df=dirty_frame, geometry=d_gpd.points_from_xy(df=dirty_frame, x='Longitude', y='Latitude', crs=crs))

def _ais_df_initial_cleaning(dirty_dataframe: d_gpd.GeoDataFrame) -> d_gpd.GeoDataFrame:
    print('Number of rows in dataframe before initial clean: ' + str(len(dirty_dataframe.index)))
    dirty_dataframe = dirty_dataframe.query(expr=(
                                '(Draught < 28.5 | Draught.isna()) & '
                                '(Width < 75) & '
                                '(Length < 488) & '
                                '(MMSI < 990000000) & '
                                '(MMSI > 99999999) & '
                                '(MMSI <= 111000000 | MMSI >= 112000000)'
                                )).compute()
    dirty_dataframe = d_gpd.from_geopandas(data=dirty_dataframe, npartitions=NUM_PARTITIONS)
    print('Number of rows in dataframe after initial clean: ' + str(len(dirty_dataframe.index)))
    return dirty_dataframe

def _create_pandas_postgresql_connection(config):
    """
    Creates a connection to the database using SQLalchemy as it is the only connection type supported by pandas
    """
    host, port = config['Database']['host'].split(':')
    connection_url = f"postgresql://{config['Database']['user']}:{config['Database']['password']}@{host}:{port}/{config['Database']['database']}"
    return create_engine(connection_url)