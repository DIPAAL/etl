import pytest
import geopandas as gpd
import pandas as pd
import pandas.api.types as ptypes
from datetime import datetime
from etl.trajectory.builder import build_from_geopandas, _rebuild_to_geodataframe, _euclidian_dist, _create_trajectory_db_df, _check_outlier, _PointCompare
from etl.helper_functions import apply_datetime_if_not_none
from etl.constants import CVS_TIMESTAMP_FORMAT, LONGITUDE_COL, LATITUDE_COL, SOG_COL, TIMESTAMP_COL, AIS_TIMESTAMP_COL, ETA_COL

CLEAN_DATA_CSV='tests/data/clean_df.csv'
ANE_LAESOE_FERRY_DATA='tests/data/ferry.csv'


def create_geopandas_dataframe() -> gpd.GeoDataFrame:
    pandas_df = pd.read_csv(CLEAN_DATA_CSV)
    pandas_df[TIMESTAMP_COL] = pandas_df[AIS_TIMESTAMP_COL].apply(func=lambda t: apply_datetime_if_not_none(t, CVS_TIMESTAMP_FORMAT))
    pandas_df[ETA_COL] = pandas_df[ETA_COL].apply(func=lambda t: apply_datetime_if_not_none(t, CVS_TIMESTAMP_FORMAT))
    pandas_df.drop(labels=[AIS_TIMESTAMP_COL], axis='columns', inplace=True)
    return _rebuild_to_geodataframe(pandas_dataframe=pandas_df)

def test_builder():
    gdf = create_geopandas_dataframe()
    result_frame = build_from_geopandas(gdf)
    print(result_frame)

euclidean_testdata = [
    (0, 0, 0, 0, 0), # a_long, a_lat, b_long, b_lat, expected
    (1, 1, 1, 1, 0),
    (0, 0, 0, 1, 1),
    (0, 0, 1, 0, 1),
    (0, 1, 0, 0, 1),
    (1, 0, 0, 0, 1),
    (0, 0, 1, 1, 1.4142135623730951)
    ]

@pytest.mark.parametrize("a_long, a_lat, b_long, b_lat, expected", euclidean_testdata)
def test_euclidian_dist(a_long, a_lat, b_long, b_lat, expected):
    assert _euclidian_dist(a_long, a_lat, b_long, b_lat) == expected


def test_create_trajectory_db_df():
    test_df = _create_trajectory_db_df()
    columns_dtype_int64 = ['start_date_id', 'start_time_id', 'end_date_id', 'end_time_id', 'eta_date_id', 'eta_time_id', 'imo', 'mmsi', ]
    columns_dtype_float64 = ['draught', 'a', 'b', 'c', 'd']
    columns_dtype_object = ['nav_status', 'trajectory', 'destination', 'rot', 'heading', 'mobile_type', 'ship_type', 'ship_name', 'ship_callsign']
    columns_dtype_timedelta = ['duration']
    columns_dtype_bool = ['infer_stopped']

    assert all([ptypes.is_int64_dtype(test_df[col]) for col in columns_dtype_int64])
    assert all([ptypes.is_float_dtype(test_df[col]) for col in columns_dtype_float64])
    assert all([ptypes.is_object_dtype(test_df[col]) for col in columns_dtype_object])
    assert all([ptypes.is_timedelta64_dtype(test_df[col]) for col in columns_dtype_timedelta])
    assert all([ptypes.is_bool_dtype(test_df[col]) for col in columns_dtype_bool])


def test_PointCompare():
    pc = _PointCompare(pointcompare_to_pd_series(11.7168, 56.8079, '07/09/2021 00:00:00', 2.5))
    
    assert pc.get_lat() == 56.8079
    assert pc.get_long() == 11.7168
    assert pc.get_time() == datetime.strptime('07/09/2021 00:00:00', CVS_TIMESTAMP_FORMAT)
    assert pc.get_sog() == 2.5

def pointcompare_to_pd_series(long:float, lat:float, timestamp:str, sog:float):

    test_frame = pd.DataFrame(data={
        LONGITUDE_COL: pd.Series(data=long, dtype='float64'),
        LATITUDE_COL: pd.Series(data=lat, dtype='float64'),
        TIMESTAMP_COL: pd.Series(data=datetime.strptime(timestamp, CVS_TIMESTAMP_FORMAT), dtype='object'),
        SOG_COL: pd.Series(data=sog, dtype='float64')
    })
    return test_frame.iloc[0]

test_data_is_outlier = [
        (_PointCompare(pointcompare_to_pd_series(56.8079,11.7168, "07/09/2021 00:00:00", 2.5)), _PointCompare(pointcompare_to_pd_series(55.8079,10.7168, "07/09/2021 00:00:00", 2.5)), 100, _euclidian_dist, True), #Same timestammp
        (_PointCompare(pointcompare_to_pd_series(56.8079,11.7168, "07/09/2021 00:00:00", 2.5)), _PointCompare(pointcompare_to_pd_series(57.8079,12.7168, "07/09/2021 00:06:02", 2.5)), 1, _euclidian_dist, True), #SOG is above threshold
        (_PointCompare(pointcompare_to_pd_series(56.8079,11.7168, "07/09/2021 00:00:00", 2.5)), _PointCompare(pointcompare_to_pd_series(56.8079,11.7168, "07/09/2021 00:00:00", 2.5)), 100, _euclidian_dist, True), #All is well
        ]

@pytest.mark.parametrize("prev_point, curr_point, speed_tresshold, distance_func, expected", test_data_is_outlier)
def test_check_outlier(prev_point, curr_point, speed_tresshold, distance_func, expected):
    assert _check_outlier(prev_point, curr_point, speed_tresshold, distance_func) == expected

