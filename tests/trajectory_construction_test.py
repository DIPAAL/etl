import pytest
import geopandas as gpd
import pandas as pd
import pandas.api.types as ptypes
from datetime import datetime

from etl.cleaning.clean_data import clean_data, create_dirty_df_from_ais_cvs
from etl.trajectory.builder import build_from_geopandas, rebuild_to_geodataframe, _euclidian_dist, \
    _create_trajectory_db_df, _check_outlier, _extract_date_smart_id, _extract_time_smart_id, _find_most_recurring, \
    POINTS_FOR_TRAJECTORY_THRESHOLD, _finalize_trajectory
from etl.constants import COORDINATE_REFERENCE_SYSTEM, CVS_TIMESTAMP_FORMAT, LONGITUDE_COL, LATITUDE_COL, SOG_COL, \
    TIMESTAMP_COL, TIMESTAMP_COL, ETA_COL
from etl.constants import T_START_DATE_COL, T_START_TIME_COL, T_END_DATE_COL, T_END_TIME_COL, T_ETA_DATE_COL, \
    T_ETA_TIME_COL, T_INFER_STOPPED_COL, T_A_COL, T_B_COL, T_C_COL, T_D_COL, T_IMO_COL, T_ROT_COL, T_MMSI_COL, \
    T_TRAJECTORY_COL, T_DESTINATION_COL, T_DURATION_COL, T_HEADING_COL, T_DRAUGHT_COL, T_MOBILE_TYPE_COL, \
    T_SHIP_TYPE_COL, T_SHIP_NAME_COL, T_SHIP_CALLSIGN_COL, T_NAVIGATIONAL_STATUS_COL

CLEAN_DATA_CSV = 'tests/data/clean_df.csv'
ANE_LAESOE_FERRY_DATA = 'tests/data/ferry.csv'

euclidean_testdata = [
    (0, 0, 0, 0, 0),  # a_long, a_lat, b_long, b_lat, expected
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
    columns_dtype_int64 = [T_START_DATE_COL, T_START_TIME_COL, T_END_DATE_COL, T_END_TIME_COL, T_ETA_DATE_COL,
                           T_ETA_TIME_COL, T_IMO_COL, T_MMSI_COL]
    columns_dtype_float64 = [T_DRAUGHT_COL, T_A_COL, T_B_COL, T_C_COL, T_D_COL]
    columns_dtype_object = [T_NAVIGATIONAL_STATUS_COL, T_TRAJECTORY_COL, T_DESTINATION_COL, T_ROT_COL, T_HEADING_COL,
                            T_MOBILE_TYPE_COL, T_SHIP_TYPE_COL, T_SHIP_NAME_COL, T_SHIP_CALLSIGN_COL]
    columns_dtype_timedelta = [T_DURATION_COL]
    columns_dtype_bool = [T_INFER_STOPPED_COL]

    assert all([ptypes.is_int64_dtype(test_df[col]) for col in columns_dtype_int64])
    assert all([ptypes.is_float_dtype(test_df[col]) for col in columns_dtype_float64])
    assert all([ptypes.is_object_dtype(test_df[col]) for col in columns_dtype_object])
    assert all([ptypes.is_timedelta64_dtype(test_df[col]) for col in columns_dtype_timedelta])
    assert all([ptypes.is_bool_dtype(test_df[col]) for col in columns_dtype_bool])


def to_minimal_outlier_detection_frame(long: float, lat: float, timestamp: str, sog: float):
    test_frame = pd.DataFrame(data={
        LONGITUDE_COL: pd.Series(data=long, dtype='float64'),
        LATITUDE_COL: pd.Series(data=lat, dtype='float64'),
        TIMESTAMP_COL: pd.Series(data=datetime.strptime(timestamp, CVS_TIMESTAMP_FORMAT), dtype='object'),
        SOG_COL: pd.Series(data=sog, dtype='float64')
    })
    geo_test_frame = gpd.GeoDataFrame(data=test_frame, geometry=gpd.points_from_xy(x=test_frame[LONGITUDE_COL],
                                                                                   y=test_frame[LONGITUDE_COL],
                                                                                   crs=COORDINATE_REFERENCE_SYSTEM))

    return geo_test_frame.iloc[[0]]


test_data_is_outlier = [
    (to_minimal_outlier_detection_frame(56.8079, 11.7168, '07/09/2021 00:00:00', 2.5),
     to_minimal_outlier_detection_frame(55.8079, 10.7168, '07/09/2021 00:00:00', 2.5), 100, _euclidian_dist, True),
    # Same timestammp
    (to_minimal_outlier_detection_frame(56.8079, 11.7168, '07/09/2021 00:00:00', 2.5),
     to_minimal_outlier_detection_frame(57.8079, 12.7168, '07/09/2021 00:06:02', 2.5), 1, _euclidian_dist, True),
    # SOG is above threshold
    (to_minimal_outlier_detection_frame(56.8079, 11.7168, '07/09/2021 00:00:00', 2.5),
     to_minimal_outlier_detection_frame(56.8079, 11.7168, '07/09/2021 00:00:00', 2.5), 100, _euclidian_dist, True),
    # All is well
]


@pytest.mark.parametrize('prev_point, curr_point, speed_threshold, distance_func, expected', test_data_is_outlier)
def test_check_outlier(prev_point, curr_point, speed_threshold, distance_func, expected):
    assert _check_outlier(prev_point, curr_point, speed_threshold, distance_func) == expected


test_data_date_smart_key_extraction = [
    (datetime.strptime('01/01/2022 00:00:00', CVS_TIMESTAMP_FORMAT), 20220101),
    (datetime.strptime('02/01/2022 00:00:00', CVS_TIMESTAMP_FORMAT), 20220102),
    (datetime.strptime('01/02/2022 00:00:00', CVS_TIMESTAMP_FORMAT), 20220201),
    (datetime.strptime('07/09/2021 00:00:00', CVS_TIMESTAMP_FORMAT), 20210907),
    (datetime.strptime('31/01/2022 10:10:20', CVS_TIMESTAMP_FORMAT), 20220131),  # Show that time does not matter
    (datetime.strptime('31/01/2022 13:14:15', CVS_TIMESTAMP_FORMAT), 20220131)  # Show that time does not matter
]


@pytest.mark.parametrize('date, expected_smart_key', test_data_date_smart_key_extraction)
def test_date_smart_key_extraction(date, expected_smart_key):
    assert _extract_date_smart_id(date) == expected_smart_key


test_data_time_smart_key_extraction = [
    (datetime.strptime('01/01/2022 00:00:00', CVS_TIMESTAMP_FORMAT), 0),
    (datetime.strptime('01/01/2022 00:00:01', CVS_TIMESTAMP_FORMAT), 1),
    (datetime.strptime('01/01/2022 00:00:10', CVS_TIMESTAMP_FORMAT), 10),
    (datetime.strptime('01/01/2022 00:01:00', CVS_TIMESTAMP_FORMAT), 100),
    (datetime.strptime('01/01/2022 00:10:00', CVS_TIMESTAMP_FORMAT), 1000),
    (datetime.strptime('01/01/2022 01:00:00', CVS_TIMESTAMP_FORMAT), 10000),
    (datetime.strptime('01/01/2022 10:00:00', CVS_TIMESTAMP_FORMAT), 100000),
    (datetime.strptime('01/01/2022 11:11:11', CVS_TIMESTAMP_FORMAT), 111111),
    (datetime.strptime('01/01/2022 12:34:56', CVS_TIMESTAMP_FORMAT), 123456),
    (datetime.strptime('01/01/2022 10:10:10', CVS_TIMESTAMP_FORMAT), 101010),  # Show that date does not matter
    (datetime.strptime('31/01/2022 10:10:10', CVS_TIMESTAMP_FORMAT), 101010),  # Show that date does not matter
    (datetime.strptime('24/12/2022 10:10:10', CVS_TIMESTAMP_FORMAT), 101010)  # Show that date does not matter
]


@pytest.mark.parametrize('time, expected_smart_key', test_data_time_smart_key_extraction)
def test_time_smart_key_extraction(time, expected_smart_key):
    assert _extract_time_smart_id(time) == expected_smart_key


def test_trajectory_construction_on_single_ferry():
    ferry_dataframe = rebuild_to_geodataframe(create_dirty_df_from_ais_cvs(ANE_LAESOE_FERRY_DATA).compute())
    expected_sailing_trajectories = 1  # Between ports
    expected_stopped_trajectories = 2  # At port
    expected_number_of_trajectories = expected_sailing_trajectories + expected_stopped_trajectories

    result_dataframe = build_from_geopandas(ferry_dataframe)

    assert expected_number_of_trajectories == len(result_dataframe.index)
    # Get rows where infer stopped is true
    stopped_result = result_dataframe[result_dataframe[T_INFER_STOPPED_COL]]
    assert expected_stopped_trajectories == len(stopped_result.index)
    sailing_result = result_dataframe[~result_dataframe[T_INFER_STOPPED_COL]]
    assert expected_sailing_trajectories == len(sailing_result.index)

    expected_unique_imo = 1
    expected_unique_mmsi = 1
    assert expected_unique_imo == result_dataframe[T_IMO_COL].nunique()
    assert expected_unique_mmsi == result_dataframe[T_MMSI_COL].nunique()


def test_find_most_recurring_drops_na():
    COL_1 = 'food'
    COL_2 = 'good'
    expected_with_dropping_entries = 4
    expected_without_dropping_entries = 5
    test_frame = pd.DataFrame(data={
        COL_1: pd.Series(data=['bacon', 'tomato', 'beans', 'fish', 'icecream']),
        COL_2: pd.Series(data=[True, False, pd.NA, False, True])
    })

    result = _find_most_recurring(dataframe=test_frame, column_subset=[COL_1, COL_2], drop_na=True)
    assert len(result.index) == expected_with_dropping_entries

    result = _find_most_recurring(dataframe=test_frame, column_subset=[COL_1, COL_2], drop_na=False)
    assert len(result.index) == expected_without_dropping_entries


def test_find_most_recurring_only_subset():
    COL_1 = 'food'
    COL_2 = 'good'
    test_frame = pd.DataFrame(data={
        COL_1: pd.Series(data=['bacon', 'tomato', 'veal', 'fish', 'icecream']),
        COL_2: pd.Series(data=[True, False, True, False, True])
    })

    result = _find_most_recurring(dataframe=test_frame, column_subset=[COL_2], drop_na=False)
    assert COL_1 not in result.columns
    assert COL_2 in result.columns

    result = _find_most_recurring(dataframe=test_frame, column_subset=[COL_1, COL_2], drop_na=False)
    assert COL_1 in result.columns
    assert COL_2 in result.columns

    with pytest.raises(ValueError):
        _find_most_recurring(dataframe=test_frame, column_subset=[], drop_na=False)


def test_point_to_trajectory_threshold_under_returns_empty_frame():
    from_idx = 4
    to_idx = 5
    assert to_idx - from_idx < POINTS_FOR_TRAJECTORY_THRESHOLD
    test_mmsi = 0
    expected_dataframe_size = 0

    result_dataframe = _finalize_trajectory(mmsi=test_mmsi, trajectory_dataframe=None, from_idx=from_idx, to_idx=to_idx,
                                            infer_stopped=False)

    assert expected_dataframe_size == len(result_dataframe.index)


def test_point_to_trajectory_threshold_on_returns_empty_frame():
    from_idx = 0
    to_idx = 2
    assert to_idx - from_idx == POINTS_FOR_TRAJECTORY_THRESHOLD
    test_mmsi = 219000734
    expected_dataframe_size = 0

    result_frame = _finalize_trajectory(mmsi=test_mmsi, trajectory_dataframe=None, from_idx=from_idx, to_idx=to_idx,
                                        infer_stopped=False)

    assert expected_dataframe_size == len(result_frame.index)


def test_point_to_trajectory_threshold_above_returns_trajectory():
    from_idx = 0
    to_idx = 10
    assert to_idx - from_idx > POINTS_FOR_TRAJECTORY_THRESHOLD
    test_mmsi = 219000734
    test_dataframe = rebuild_to_geodataframe(create_dirty_df_from_ais_cvs(ANE_LAESOE_FERRY_DATA).compute())
    test_dataframe = test_dataframe.iloc[from_idx:to_idx]
    expected_dataframe_size = 1

    result_frame = _finalize_trajectory(mmsi=test_mmsi, trajectory_dataframe=test_dataframe, from_idx=from_idx,
                                        to_idx=to_idx, infer_stopped=False)

    assert expected_dataframe_size == len(result_frame.index)
