"""Module responsible for cleaning raw AIS data points."""
import geopandas as gpd
import dask.dataframe as dd
import dask_geopandas as d_gpd
import multiprocessing
from etl.helper_functions import wrap_with_timings, get_first_query_in_file
from etl.audit.logger import global_audit_logger as gal, ROWS_KEY, DEBUG_KEY
from sqlalchemy import create_engine
from etl.constants import COORDINATE_REFERENCE_SYSTEM, CVS_TIMESTAMP_FORMAT, TIMESTAMP_COL, ETA_COL, LONGITUDE_COL, \
    LATITUDE_COL, CARGO_TYPE_COL, DESTINATION_COL, CALLSIGN_COL, NAME_COL, A_COL, B_COL, C_COL, D_COL, WIDTH_COL, \
    SOG_COL, ROT_COL, MMSI_COL, LENGTH_COL, HEADING_COL, DRAUGHT_COL, IMO_COL, COG_COL, SHIP_TYPE_COL, \
    ETL_STAGE_SPATIAL, POSITION_FIXING_DEVICE_COL, MOBILE_TYPE_COL

CSV_EXTENSION = '.csv'
GEOMETRY_BOUNDS_QUERY = './etl/cleaning/sql/geometry_bounds.sql'
# Specifies the number of partition for DAsk Dataframes based on the number of CPU cores available
NUM_PARTITIONS = 4 * multiprocessing.cpu_count()


def clean_data(config, ais_file_path: str) -> gpd.GeoDataFrame:
    """
    Read AIS data from a file and returns the cleaned data.

    Raises exception if file extension is not supported.
    Currently supported extensions are: .csv

    Keyword arguments:
        config: the application configuration
        ais_file_path: the absolute or relative file path to AIS data file
    """
    if ais_file_path.endswith(CSV_EXTENSION):

        return _clean_csv_data(config, ais_file_path)

    raise NotImplementedError(
        f'Extension of file provided {ais_file_path}, is not supported in this version of the project.'
    )


def _clean_csv_data(config, ais_file_path_csv: str) -> gpd.GeoDataFrame:
    """Read AIS data from a CSV file and returns the cleaned data.

    Keyword arguments:
        config: the application configuration
        ais_file_path_csv: the absolute or relative file path to the AIS data csv file
    """
    # Use Geopandas and psycopg2 to get the Danish Waters geometry from the DB.
    danish_waters_gdf = wrap_with_timings('Fetch Danish Waters', lambda: _get_danish_waters_boundary(config))

    # Read from georeferenced AIS dataframe from csv file
    dirty_geo_dataframe = wrap_with_timings(
        'Create Geodataframe from CSV',
        lambda: create_dirty_df_from_ais_csv(csv_path=ais_file_path_csv)
    )

    # Initial cleaning of AIS dataframe
    initial_cleaned_dataframe = wrap_with_timings(
        'Initial clean',
        lambda: _ais_df_initial_cleaning(dirty_dataframe=dirty_geo_dataframe).compute()
    )

    # Do a spatial join (inner join) to find all the ships that is within the boundary of danish_waters
    lazy_clean = d_gpd.sjoin(initial_cleaned_dataframe, danish_waters_gdf, predicate='within')
    clean_gdf = wrap_with_timings('Spatial cleaning', lambda: lazy_clean.compute(),
                                  audit_etl_stage=ETL_STAGE_SPATIAL
                                  )
    gal.log_dict[DEBUG_KEY][ROWS_KEY]['spatial_join'] = len(clean_gdf.index)
    print('Number of rows in boundary cleaned dataframe: ' + str(len(clean_gdf.index)))

    return clean_gdf


def _get_danish_waters_boundary(config) -> d_gpd.GeoDataFrame:
    """
    Return Danish Waters geometry bounds from the DWH.

    Keyword arguments:
        config: the application configuration
    """
    conn = _create_pandas_postgresql_connection(config)

    query = get_first_query_in_file(GEOMETRY_BOUNDS_QUERY)
    temp_waters = gpd.read_postgis(sql=query, con=conn)
    return d_gpd.from_geopandas(data=temp_waters, npartitions=1)


def create_dirty_df_from_ais_csv(csv_path: str) -> dd.DataFrame:
    """
    Return a dask dataframe containing the raw data within the csv file.

    Keyword arguments:
        csv_path: absolute or relative file path to a csv file containing AIS data
    """
    dirty_frame = dd.read_csv(
        csv_path,
        dtype={
            CALLSIGN_COL: 'object',
            CARGO_TYPE_COL: 'object',
            DESTINATION_COL: 'object',
            ETA_COL: 'object',
            NAME_COL: 'object',
            COG_COL: 'float64',
            DRAUGHT_COL: 'float64',
            HEADING_COL: 'float64',
            IMO_COL: 'object',
            POSITION_FIXING_DEVICE_COL: 'object',
            MOBILE_TYPE_COL: 'object',
            SHIP_TYPE_COL: 'object',
            NAME_COL: 'object',
            LATITUDE_COL: 'float64',
            LONGITUDE_COL: 'float64',
            LENGTH_COL: 'float64',
            MMSI_COL: 'int64',
            ROT_COL: 'float64',
            SOG_COL: 'float64',
            WIDTH_COL: 'float64',
            A_COL: 'float64',
            B_COL: 'float64',
            C_COL: 'float64',
            D_COL: 'float64',
        }
    )
    # Replace "Unknown" with nan and change type to int for imo
    dirty_frame[IMO_COL] = dd.to_numeric(dirty_frame[IMO_COL], errors='coerce')

    dirty_frame[TIMESTAMP_COL] = dd.to_datetime(dirty_frame[TIMESTAMP_COL], format=CVS_TIMESTAMP_FORMAT)
    dirty_frame[ETA_COL] = dd.to_datetime(dirty_frame[ETA_COL], format=CVS_TIMESTAMP_FORMAT)
    return dirty_frame


def _ais_df_initial_cleaning(dirty_dataframe: dd.DataFrame) -> dd.DataFrame:
    """
    Remove raw AIS data that does not conform to pre-defined non-spatial cleaning rules and return the rest.

    Keyword arguments:
        dirty_dataframe: a Dask Dataframe containing raw AIS data

    Cleaning rules
    --------------
    >>> Remove where draught >= 28.5 (keep nulls/none)
    >>> Remove where width >= 75
    >>> Remove where length >= 488
    >>> Remove where 99999999 =< MMSI >= 990000000
    >>> Remove where 112000000 < MMSI > 111000000
    """
    print(f"Number of rows in dirty dataframe: {len(dirty_dataframe)}")
    dirty_dataframe = wrap_with_timings("Initial data filter", lambda: dirty_dataframe.query(expr=(
                                '(Draught < 28.5 | Draught.isna()) & '
                                '(Width < 75) & '
                                '(Length < 488) & '
                                '(MMSI < 990000000) & '
                                '(MMSI > 99999999) & '
                                '(MMSI <= 111000000 | MMSI >= 112000000)'
                                )))
    print(f"Number of rows in initial cleaned dataframe: {len(dirty_dataframe)}")

    return wrap_with_timings(
        'Creating geodataframe',
        lambda: d_gpd.from_dask_dataframe(
            dirty_dataframe,
            geometry=d_gpd.points_from_xy(
                df=dirty_dataframe, x=LONGITUDE_COL, y=LATITUDE_COL, crs=COORDINATE_REFERENCE_SYSTEM
            )
        ).set_crs(COORDINATE_REFERENCE_SYSTEM)
    )


def _create_pandas_postgresql_connection(config):
    """
    Return a connection to the database using SQLalchemy, as it is the only connection type supported by pandas.

    Keyword arguments:
        config: the application configuration
    """
    host, port = config['Database']['host'].split(':')
    user = config['Database']['user']
    password = config['Database']['password']
    database = config['Database']['database']
    connection_url = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    return create_engine(connection_url)
