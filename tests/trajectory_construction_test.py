import pytest
import geopandas as gpd
import pandas as pd
import pandas.api.types as ptypes
from datetime import datetime
from etl.trajectory.builder import build_from_geopandas, _rebuild_to_geodataframe, _euclidian_dist, _create_trajectory_db_df, _check_outlier, _PointCompare, _extract_date_smart_id, _extract_time_smart_id
from etl.helper_functions import apply_datetime_if_not_none
from etl.constants import CVS_TIMESTAMP_FORMAT, LONGITUDE_COL, LATITUDE_COL, SOG_COL, TIMESTAMP_COL, AIS_TIMESTAMP_COL, ETA_COL
from etl.constants import T_START_DATE_COL, T_START_TIME_COL, T_END_DATE_COL, T_END_TIME_COL, T_ETA_DATE_COL, T_ETA_TIME_COL, T_INFER_STOPPED_COL, T_A_COL, T_B_COL, T_C_COL, T_D_COL, T_IMO_COL, T_ROT_COL, T_MMSI_COL, T_TRAJECTORY_COL, T_DESTINATION_COL, T_DURATION_COL, T_HEADING_COL, T_DRAUGHT_COL, T_MOBILE_TYPE_COL, T_SHIP_TYPE_COL, T_SHIP_NAME_COL, T_SHIP_CALLSIGN_COL, T_NAVIGATIONAL_STATUS_COL

CLEAN_DATA_CSV='tests/data/clean_df.csv'
ANE_LAESOE_FERRY_DATA='tests/data/ferry.csv'

def create_geopandas_dataframe(data_file_path: str) -> gpd.GeoDataFrame:
    pandas_df = pd.read_csv(data_file_path)
    pandas_df[TIMESTAMP_COL] = pandas_df[AIS_TIMESTAMP_COL].apply(func=lambda t: apply_datetime_if_not_none(t, CVS_TIMESTAMP_FORMAT))
    pandas_df[ETA_COL] = pandas_df[ETA_COL].apply(func=lambda t: apply_datetime_if_not_none(t, CVS_TIMESTAMP_FORMAT))
    pandas_df.drop(labels=[AIS_TIMESTAMP_COL], axis='columns', inplace=True)
    return _rebuild_to_geodataframe(pandas_dataframe=pandas_df)

euclidean_testdata = [
    (0, 0, 0, 0, 0), # a_long, a_lat, b_long, b_lat, expected
    (1, 1, 1, 1, 0),
    (0, 0, 0, 1, 1),
    (0, 0, 1, 0, 1),
    (0, 1, 0, 0, 1),
    (1, 0, 0, 0, 1),
    (0, 0, 1, 1, 1.4142135623730951)
    ]

@pytest.mark.parametrize('a_long, a_lat, b_long, b_lat, expected', euclidean_testdata)
def test_euclidian_dist(a_long, a_lat, b_long, b_lat, expected):
    assert _euclidian_dist(a_long, a_lat, b_long, b_lat) == expected


def test_create_trajectory_db_df():
    test_df = _create_trajectory_db_df()
    columns_dtype_int64 = [T_START_DATE_COL, T_START_TIME_COL, T_END_DATE_COL, T_END_TIME_COL, T_ETA_DATE_COL, T_ETA_TIME_COL, T_IMO_COL, T_MMSI_COL ]
    columns_dtype_float64 = [T_DRAUGHT_COL, T_A_COL, T_B_COL, T_C_COL, T_D_COL]
    columns_dtype_object = [T_NAVIGATIONAL_STATUS_COL, T_TRAJECTORY_COL, T_DESTINATION_COL, T_ROT_COL, T_HEADING_COL, T_MOBILE_TYPE_COL, T_SHIP_TYPE_COL, T_SHIP_NAME_COL, T_SHIP_CALLSIGN_COL]
    columns_dtype_timedelta = [T_DURATION_COL]
    columns_dtype_bool = [T_INFER_STOPPED_COL]

    assert all([ptypes.is_int64_dtype(test_df[col]) for col in columns_dtype_int64])
    assert all([ptypes.is_float_dtype(test_df[col]) for col in columns_dtype_float64])
    assert all([ptypes.is_object_dtype(test_df[col]) for col in columns_dtype_object])
    assert all([ptypes.is_timedelta64_dtype(test_df[col]) for col in columns_dtype_timedelta])
    assert all([ptypes.is_bool_dtype(test_df[col]) for col in columns_dtype_bool])


def test_PointCompare():
    expected_long = 11.7168
    expected_lat = 56.8079
    expected_time = '07/09/2021 00:00:00'
    expected_sog = 2.5

    pc = _PointCompare(pointcompare_to_pd_series(expected_long, expected_lat, expected_time, expected_sog))
    
    assert pc.get_lat() == expected_lat
    assert pc.get_long() == expected_long
    assert pc.get_time() == datetime.strptime(expected_time, CVS_TIMESTAMP_FORMAT)
    assert pc.get_sog() == expected_sog

def pointcompare_to_pd_series(long:float, lat:float, timestamp:str, sog:float):
    test_frame = pd.DataFrame(data={
        LONGITUDE_COL: pd.Series(data=long, dtype='float64'),
        LATITUDE_COL: pd.Series(data=lat, dtype='float64'),
        TIMESTAMP_COL: pd.Series(data=datetime.strptime(timestamp, CVS_TIMESTAMP_FORMAT), dtype='object'),
        SOG_COL: pd.Series(data=sog, dtype='float64')
    })
    return test_frame.iloc[0]

test_data_is_outlier = [
        (_PointCompare(pointcompare_to_pd_series(56.8079,11.7168, '07/09/2021 00:00:00', 2.5)), _PointCompare(pointcompare_to_pd_series(55.8079,10.7168, '07/09/2021 00:00:00', 2.5)), 100, _euclidian_dist, True), #Same timestammp
        (_PointCompare(pointcompare_to_pd_series(56.8079,11.7168, '07/09/2021 00:00:00', 2.5)), _PointCompare(pointcompare_to_pd_series(57.8079,12.7168, '07/09/2021 00:06:02', 2.5)), 1, _euclidian_dist, True), #SOG is above threshold
        (_PointCompare(pointcompare_to_pd_series(56.8079,11.7168, '07/09/2021 00:00:00', 2.5)), _PointCompare(pointcompare_to_pd_series(56.8079,11.7168, '07/09/2021 00:00:00', 2.5)), 100, _euclidian_dist, True), #All is well
        ]

@pytest.mark.parametrize('prev_point, curr_point, speed_threshold, distance_func, expected', test_data_is_outlier)
def test_check_outlier(prev_point, curr_point, speed_threshold, distance_func, expected):
    assert _check_outlier(prev_point, curr_point, speed_threshold, distance_func) == expected

test_data_date_smart_key_extraction = [
    (datetime.strptime('01/01/2022 00:00:00', CVS_TIMESTAMP_FORMAT), 20220101),
    (datetime.strptime('02/01/2022 00:00:00', CVS_TIMESTAMP_FORMAT), 20220102),
    (datetime.strptime('01/02/2022 00:00:00', CVS_TIMESTAMP_FORMAT), 20220201),
    (datetime.strptime('07/09/2021 00:00:00', CVS_TIMESTAMP_FORMAT), 20210907),
    (datetime.strptime('31/01/2022 10:10:20', CVS_TIMESTAMP_FORMAT), 20220131),
    (datetime.strptime('31/01/2022 13:14:15', CVS_TIMESTAMP_FORMAT), 20220131)
]

@pytest.mark.parametrize('date, expected_smart_key', test_data_date_smart_key_extraction)
def test_date_smart_key_extraction(date, expected_smart_key):
    assert _extract_date_smart_id(date) == expected_smart_key


def test_trajectory_construction_on_single_ferry():
    ferry_dataframe = create_geopandas_dataframe(ANE_LAESOE_FERRY_DATA)
    expected_sailing_trajectories = 1 # Between ports
    expected_stopped_trajectories = 2 # At port
    expected_number_of_trajectories = expected_sailing_trajectories + expected_stopped_trajectories

    result_dataframe = build_from_geopandas(ferry_dataframe)

    assert expected_number_of_trajectories == len(result_dataframe.index)
    stopped_result = result_dataframe.loc[result_dataframe[T_INFER_STOPPED_COL] == True]
    assert expected_stopped_trajectories == len(stopped_result.index)
    sailing_result = result_dataframe.loc[result_dataframe[T_INFER_STOPPED_COL] == False]
    assert expected_sailing_trajectories == len(sailing_result.index)

    expected_unique_imo = 1
    exåected_unique_mmsi = 1
    assert expected_unique_imo == result_dataframe[T_IMO_COL].nunique()
    assert exåected_unique_mmsi == result_dataframe[T_MMSI_COL].nunique()