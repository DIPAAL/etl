import pytest
import geopandas as gpd
import pandas as pd
import pandas.api.types as ptypes
import numpy as np

from typing import List
from datetime import datetime, timedelta
from etl.cleaning.clean_data import create_dirty_df_from_ais_csv
from etl.trajectory.builder import build_from_geopandas, rebuild_to_geodataframe, _euclidian_dist, \
    _create_trajectory_db_df, _check_outlier, extract_date_smart_id, _extract_time_smart_id, _find_most_recurring, \
    POINTS_FOR_TRAJECTORY_THRESHOLD, _finalize_trajectory, _tfloat_from_dataframe, COORDINATE_REFERENCE_SYSTEM_METERS
from etl.constants import CVS_TIMESTAMP_FORMAT, LONGITUDE_COL, LATITUDE_COL, SOG_COL, TIMESTAMP_COL, T_LENGTH_COL
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
    columns_dtype_float64 = [T_A_COL, T_B_COL, T_C_COL, T_D_COL, T_LENGTH_COL]
    columns_dtype_object = [T_DRAUGHT_COL, T_NAVIGATIONAL_STATUS_COL, T_TRAJECTORY_COL, T_DESTINATION_COL, T_ROT_COL,
                            T_HEADING_COL, T_MOBILE_TYPE_COL, T_SHIP_TYPE_COL, T_SHIP_NAME_COL, T_SHIP_CALLSIGN_COL]
    columns_dtype_timedelta = [T_DURATION_COL]
    columns_dtype_bool = [T_INFER_STOPPED_COL]

    assert all([ptypes.is_int64_dtype(test_df[col]) for col in columns_dtype_int64])
    assert all([ptypes.is_float_dtype(test_df[col]) for col in columns_dtype_float64])
    assert all([ptypes.is_object_dtype(test_df[col]) for col in columns_dtype_object])
    assert all([ptypes.is_timedelta64_dtype(test_df[col]) for col in columns_dtype_timedelta])
    assert all([ptypes.is_bool_dtype(test_df[col]) for col in columns_dtype_bool])


def to_minimal_outlier_detection_frame(long: List[float], lat: List[float], timestamps: List[int], sog: List[float]):
    test_frame = pd.DataFrame(data={
        LONGITUDE_COL: pd.Series(data=long, dtype='float64'),
        LATITUDE_COL: pd.Series(data=lat, dtype='float64'),
        # Timestamp col is 2022-01-01 00:00:00 + the value from timestamp list in seconds
        TIMESTAMP_COL: pd.Series(data=[datetime(2022, 1, 1, 0, 0, 0) + timedelta(seconds=ts) for ts in timestamps]),
        SOG_COL: pd.Series(data=sog, dtype='float64')
    })
    return gpd.GeoDataFrame(
        data=test_frame,
        geometry=gpd.points_from_xy(
            x=test_frame[LONGITUDE_COL],
            y=test_frame[LATITUDE_COL],
            crs=COORDINATE_REFERENCE_SYSTEM_METERS
        )
    )


test_data_is_outlier = [
    # Test Case 1: Same timestammp
    (to_minimal_outlier_detection_frame([0, 0], [0, 0], [0, 0], [2.5, 2.5]), 100, True),
    # Test Case 2: SOG is above threshold, where moving a bit more than 50km in 5 minutes.
    (to_minimal_outlier_detection_frame([0, 50000], [0, 50000], [0, 300], [2.5, 2.5]), 1, True),
    # Test Case 3: Not a outlier, where moving a bit more than 3km in 5 minutes.
    (to_minimal_outlier_detection_frame([0, 3000], [0, 3000], [0, 300], [2.5, 2.5]), 100, False),
]


@pytest.mark.parametrize('dataframe, speed_threshold, expected', test_data_is_outlier)  # noqa: E501
def test_check_outlier(dataframe,  speed_threshold, expected):
    distance_func = _euclidian_dist

    prev_point = (0, dataframe.loc[0])
    curr_point = (1, dataframe.loc[1])

    assert _check_outlier(dataframe, curr_point, prev_point, speed_threshold, distance_func) == expected


test_data_date_smart_key_extraction = [
    (datetime.strptime('01/01/2022 00:00:00', CVS_TIMESTAMP_FORMAT), 20220101),
    (datetime.strptime('02/01/2022 00:00:00', CVS_TIMESTAMP_FORMAT), 20220102),
    (datetime.strptime('01/02/2022 00:00:00', CVS_TIMESTAMP_FORMAT), 20220201),
    (datetime.strptime('07/09/2021 00:00:00', CVS_TIMESTAMP_FORMAT), 20210907),
    (datetime.strptime('31/01/2022 10:10:20', CVS_TIMESTAMP_FORMAT), 20220131),  # Show that time does not matter
    (datetime.strptime('31/01/2022 13:14:15', CVS_TIMESTAMP_FORMAT), 20220131),  # Show that time does not matter
    (None, -1)
]


@pytest.mark.parametrize('date, expected_smart_key', test_data_date_smart_key_extraction)
def test_date_smart_key_extraction(date, expected_smart_key):
    assert extract_date_smart_id(date) == expected_smart_key


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
    (datetime.strptime('24/12/2022 10:10:10', CVS_TIMESTAMP_FORMAT), 101010),  # Show that date does not matter
    (None, -1)
]


@pytest.mark.parametrize('time, expected_smart_key', test_data_time_smart_key_extraction)
def test_time_smart_key_extraction(time, expected_smart_key):
    assert _extract_time_smart_id(time) == expected_smart_key


def test_trajectory_construction_on_single_ferry():
    ferry_dataframe = rebuild_to_geodataframe(create_dirty_df_from_ais_csv(ANE_LAESOE_FERRY_DATA).compute())
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
    test_dataframe = rebuild_to_geodataframe(create_dirty_df_from_ais_csv(ANE_LAESOE_FERRY_DATA).compute())
    test_dataframe = test_dataframe.iloc[from_idx:to_idx]
    expected_dataframe_size = 1

    result_frame = _finalize_trajectory(mmsi=test_mmsi, trajectory_dataframe=test_dataframe, from_idx=from_idx,
                                        to_idx=to_idx, infer_stopped=False)

    assert expected_dataframe_size == len(result_frame.index)


def test_point_to_trajectory_correct_length():
    ferry_dataframe = rebuild_to_geodataframe(create_dirty_df_from_ais_csv(ANE_LAESOE_FERRY_DATA).compute())
    result_dataframe = build_from_geopandas(ferry_dataframe)
    total_length = result_dataframe[T_LENGTH_COL].sum()

    expected_length = 27345

    assert abs(total_length - expected_length) < 1


def create_nan_test_dataframe(float_values: List[float]) -> gpd.GeoDataFrame:
    col_1 = 'col_1'
    values_size = len(float_values)
    timestamp_list = pd.date_range(start='01/01/2022 00:00:00', periods=values_size).to_list()
    latitude_list = [10] * values_size
    longitude_list = [57] * values_size

    dataframe: pd.DataFrame = pd.DataFrame(data={
        col_1: pd.Series(data=float_values, dtype='float64'),
        TIMESTAMP_COL: pd.Series(data=timestamp_list, dtype='object'),
        LONGITUDE_COL: pd.Series(data=longitude_list, dtype='float64'),
        LATITUDE_COL: pd.Series(data=latitude_list, dtype='float64')
    })
    dataframe[TIMESTAMP_COL] = pd.to_datetime(dataframe[TIMESTAMP_COL], format='%Y-%m-%d %H:%M:%S')
    return rebuild_to_geodataframe(dataframe)


test_nan_removal_data = [
    (create_nan_test_dataframe([13, 14, 15, np.nan, 17, 29]), True, False, 5),
    (create_nan_test_dataframe([np.nan, 13, 7.43, 3.14, np.nan, 10]), False, True, 4),
    (create_nan_test_dataframe([1, 2, 3, 4, 5, np.nan]), False, False, 6)
]


@pytest.mark.parametrize('test_frame, defaulted, remove, expected_number_of_values', test_nan_removal_data)
def test_nan_values_removed(test_frame, defaulted, remove, expected_number_of_values):
    float_column = 'col_1'
    original_num_values = len(test_frame)

    actual = _tfloat_from_dataframe(test_frame, float_column=float_column) if defaulted else \
        _tfloat_from_dataframe(test_frame, float_column=float_column, remove_nan=remove)

    assert expected_number_of_values == actual.numInstants
    assert original_num_values == len(test_frame)  # Test original frame has not been changed
