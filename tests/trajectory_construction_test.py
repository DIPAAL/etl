import pytest
from etl.trajectory.builder import build_from_geopandas, _rebuild_to_geodataframe, CVS_TIMESTAMP_FORMAT, _euclidian_dist, _create_trajectory_db_df, _check_outlier, _PointCompare
import geopandas as gpd
import pandas as pd
import pandas.api.types as ptypes
from datetime import datetime

CLEAN_DATA_CSV='tests/data/clean_df.csv'


def apply_datetime_if_not_none(str_in):
    try:
        d = datetime.strptime(str_in, CVS_TIMESTAMP_FORMAT)
    except Exception:
        d = None
    return d

def create_geopandas_dataframe() -> gpd.GeoDataFrame:
    pandas_df = pd.read_csv(CLEAN_DATA_CSV)
    pandas_df['Timestamp'] = pandas_df['# Timestamp'].apply(func=apply_datetime_if_not_none)
    pandas_df['ETA'] = pandas_df['ETA'].apply(func=apply_datetime_if_not_none)
    pandas_df.drop(labels=['# Timestamp'], axis='columns', inplace=True)
    return _rebuild_to_geodataframe(pandas_dataframe=pandas_df)

def test_builder():
    gdf = create_geopandas_dataframe()
    result_frame = build_from_geopandas(gdf)
    print(result_frame)
    # Always make it stop, in order to see the prints

testdata = [
    (0, 0, 0, 0, 0),
    (1, 1, 1, 1, 0),
    (0, 0, 0, 1, 1),
    (0, 0, 1, 0, 1),
    (0, 1, 0, 0, 1),
    (1, 0, 0, 0, 1)
    ]

@pytest.mark.parametrize("a_long, a_lat, b_long, b_lat, expected", testdata)
def test_euclidian_dist(a_long, a_lat, b_long, b_lat, expected):
    assert _euclidian_dist(a_long, a_lat, b_long, b_lat) == expected


def test_create_trajectory_db_df():
    test_df = _create_trajectory_db_df()
    columns_dtype_int64 = ['start_data_id', 'start_time_id', 'end_data_id', 'end_time_id', 'eta_date_id', 'eta_time_id'] 
    columns_dtype_float64 = ['draught']
    columns_dtype_object = ['nav_status', 'trajectory', 'destination', 'rot', 'heading']
    columns_dtype_timedelta = ['duration']
    columns_dtype_bool = ['infer_stopped']

    assert all([ptypes.is_int64_dtype(test_df[col]) for col in columns_dtype_int64])
    assert all([ptypes.is_float64_dtype(test_df[col]) for col in columns_dtype_float64])
    assert all([ptypes.is_object_dtype(test_df[col]) for col in columns_dtype_object])
    assert all([ptypes.is_timedelta64_dtype(test_df[col]) for col in columns_dtype_timedelta])
    assert all([ptypes.is_bool_dtype(test_df[col]) for col in columns_dtype_bool])


def test_PointCompare():
    pc = _PointCompare(56.8079,11.7168, "07/09/2021 00:00:00", 2.5)
    
    assert pc.get_lat() == 56.8079
    assert pc.get_long() == 11.7168
    assert pc.get_time() == "07-09-2021 00:00:00+1"
    assert pc.get_sog() == 2.5

def pointcompare_to_pd_series(long:float, lat:float, timestamp:str, sog:float):
    return pd.Series(data={
        'Longitude': long,
        'Latitude': lat,
        'Timestamp': datetime.strptime(timestamp, CVS_TIMESTAMP_FORMAT),
        'SOG': sog
    }).to_frame().T

test_data_is_outlier = [
        (_PointCompare(pointcompare_to_pd_series(56.8079,11.7168, "07/09/2021 00:00:00", 2.5)), _PointCompare(pointcompare_to_pd_series(55.8079,10.7168, "07/09/2021 00:00:00", 2.5)), 100, _euclidian_dist, True), #Same timestammp
        (_PointCompare(pointcompare_to_pd_series(56.8079,11.7168, "07/09/2021 00:00:00", 2.5)), _PointCompare(pointcompare_to_pd_series(57.8079,12.7168, "07/09/2021 00:06:02", 2.5)), 1, _euclidian_dist, True), #SOG is above threshold
        (_PointCompare(pointcompare_to_pd_series(56.8079,11.7168, "07/09/2021 00:00:00", 2.5)), _PointCompare(pointcompare_to_pd_series(56.8079,11.7168, "07/09/2021 00:00:00", 2.5)), 100, _euclidian_dist, True), #All is well
        ]

@pytest.mark.parametrize("prev_point, curr_point, speed_tresshold, distance_func, expected", test_data_is_outlier)
def test_check_outlier(prev_point, curr_point, speed_tresshold, distance_func, expected):
    assert _check_outlier(prev_point, curr_point, speed_tresshold, distance_func) == expected

