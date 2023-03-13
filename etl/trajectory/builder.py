"""Module handling trajectory construction and outlier detection."""

from concurrent.futures import ProcessPoolExecutor
import geopandas as gpd
import numpy as np
import pandas as pd
import math
from datetime import datetime
from mobilitydb import TGeomPointSeq, TFloatInst, TFloatSeq
from typing import Callable, Optional, List, Tuple
from etl.constants import COORDINATE_REFERENCE_SYSTEM, LONGITUDE_COL, LATITUDE_COL, TIMESTAMP_COL, SOG_COL, MMSI_COL, \
    ETA_COL, DESTINATION_COL, NAVIGATIONAL_STATUS_COL, DRAUGHT_COL, ROT_COL, HEADING_COL, IMO_COL, \
    POSITION_FIXING_DEVICE_COL, SHIP_TYPE_COL, NAME_COL, CALLSIGN_COL, A_COL, B_COL, C_COL, D_COL, \
    MBDB_TRAJECTORY_COL, GEO_PANDAS_GEOMETRY_COL, LOCATION_SYSTEM_TYPE_COL, T_LOCATION_SYSTEM_TYPE_COL, \
    TRAJECTORY_SRID, MOBILE_TYPE_COL, T_POSITION_FIXING_DEVICE_COL, UNKNOWN_INT_VALUE
from etl.constants import T_INFER_STOPPED_COL, T_DURATION_COL, T_C_COL, T_D_COL, T_TRAJECTORY_COL, T_DESTINATION_COL, \
    T_ROT_COL, T_HEADING_COL, T_MMSI_COL, T_IMO_COL, T_B_COL, T_A_COL, T_MOBILE_TYPE_COL, T_SHIP_TYPE_COL, \
    T_SHIP_NAME_COL, T_SHIP_CALLSIGN_COL, T_NAVIGATIONAL_STATUS_COL, T_DRAUGHT_COL, T_ETA_TIME_COL, T_ETA_DATE_COL, \
    T_START_TIME_COL, T_START_DATE_COL, T_END_TIME_COL, T_END_DATE_COL
from tqdm import tqdm

from etl.helper_functions import extract_smart_date_id_from_date

SPEED_THRESHOLD_KNOTS = 100

MOBILITYDB_TIMESTAMP_FORMAT = '%Y-%m-%d %H:%M:%S'  # 2020-01-01 00:00:00+01
COORDINATE_REFERENCE_SYSTEM_METERS = 'epsg:3034'
KNOTS_PER_METER_SECONDS = 1.943844  # = 1 m/s
COMPUTED_VS_SOG_KNOTS_THRESHOLD = 2
STOPPED_KNOTS_THRESHOLD = 0.5
STOPPED_TIME_SECONDS_THRESHOLD = 5 * 60  # 5 minutes
SPLIT_GAP_SECONDS_THRESHOLD = 5 * 60  # 5 minutes
POINTS_FOR_TRAJECTORY_THRESHOLD = 2  # P=2
UNKNOWN_STRING_VALUE = 'Unknown'
UNKNOWN_FLOAT_VALUE = -1.0

AIS_LONGEST_REPORTING_RATE_MIN = 3  # 3 min
POINT_TIME_DIFFERENCE_SPLIT_THRESHOLD = (AIS_LONGEST_REPORTING_RATE_MIN * 5) * 60  # 3 min * 5 = 15 min = 900 seconds


def build_from_geopandas(clean_sorted_ais: gpd.GeoDataFrame) -> pd.DataFrame:
    """
    Build and return a pandas dataframe containing built trajectories based on the provided AIS data.

    Keyword arguments:
        clean_sorted_ais: A GeoDataFrame of cleaned and ascending timestamp sorted AIS data.
    """
    grouped_data = clean_sorted_ais.groupby(by=MMSI_COL)

    # https://gist.github.com/alexeygrigorev/79c97c1e9dd854562df9bbeea76fc5de
    # Build trajectories in parallel
    with ProcessPoolExecutor() as pool:
        with tqdm(total=len(grouped_data)) as progress:
            futures = []

            for group in grouped_data:
                future = pool.submit(_create_trajectory, group)
                future.add_done_callback(lambda p: progress.update())
                futures.append(future)

            results = []
            for future in futures:
                result = future.result()
                results.append(result)

    df = pd.concat(results)
    df.loc[:, T_ROT_COL].mask(df.loc[:, T_ROT_COL].isna(), other=None, inplace=True)
    df.loc[:, T_HEADING_COL].mask(df.loc[:, T_HEADING_COL].isna(), other=None, inplace=True)
    df.loc[:, T_DRAUGHT_COL] = df.loc[:, T_DRAUGHT_COL].astype('object').mask(df.loc[:, T_DRAUGHT_COL].isna(),
                                                                              other=None)
    df.loc[:, T_DESTINATION_COL].mask(df.loc[:, T_DESTINATION_COL].isna(), other="UNKNOWN DESTINATION", inplace=True)
    return df


def _create_trajectory(grouped_data) -> pd.DataFrame:
    """
    Create and return trajectories for a single ship identified by MMSI as a pandas dataframe.

    During creation AIS outlier points are detected and removed.

    Keyword arguments:
        grouped_data: a DataFrameGroupBy on MMSI of AIS point data
    """
    mmsi, data = grouped_data

    # Sort the data by timestamp
    data = data.sort_values(by=TIMESTAMP_COL)

    dataframe = _remove_outliers(dataframe=data)
    # Reset the index as some rows might have been classified as outliers and removed
    dataframe.reset_index(inplace=True)

    return _construct_moving_trajectory(mmsi, dataframe, 0)


def _constraint_time_difference(cur_row: gpd.GeoSeries, prev_row: gpd.GeoSeries) -> bool:
    """
    Check if time difference constraint between consequtive AIS point is satisfied.

    Attributes:
        cur_row: the row containing data for the current AIS data point for the trajectory
        prev_row: the row containing data for the previous AIS data point in the trajectory
    """
    # If there is no previous row, then do not enforce constraint
    if prev_row is None:
        return False

    time_diff_dt = cur_row[TIMESTAMP_COL] - prev_row[TIMESTAMP_COL]
    return time_diff_dt.seconds >= POINT_TIME_DIFFERENCE_SPLIT_THRESHOLD


def _update_cannot_handle(cur_row: gpd.GeoSeries, cur_idx: int, cannot_handle: Tuple[int, datetime]) \
        -> Tuple[int, datetime]:
    """
    Update stopped index based on speed constraint.

    Keyword arguments:
        cur_row: the row containing data for the current AIS data point for the trajectory
        cur_idx: index at the currently examined AIS data point
        cannot_handle: original tuple indicating if a ship is suspected of being stopped
    """
    if cur_row[SOG_COL] >= STOPPED_KNOTS_THRESHOLD:
        # Reset any possible stop because we are currently moving
        return None
    elif cannot_handle is None:
        # We have a new possible stop
        return (cur_idx, cur_row[TIMESTAMP_COL])
    else:
        # We already have a possible stop
        return cannot_handle


def _construct_moving_trajectory(mmsi: int, trajectory_dataframe: gpd.GeoDataFrame, from_idx: int) -> pd.DataFrame:
    """
    Construct and returns trajectories from the AIS data as a pandas dataframe.

    Keyword arguments:
        mmsi: the maritime mobile mervice identity used to identify a ship
        trajectory_dataframe: geopandas dataframe containing AIS points for a single ship
        from_idx: the index to start creating trajectories from
    """
    cannot_handle = None
    prev_row = None
    for idx, row in trajectory_dataframe.iloc[from_idx:].iterrows():
        cannot_handle = _update_cannot_handle(row, idx, cannot_handle)
        # Has the ship possibly stopped?
        if cannot_handle is not None:
            current_date = row[TIMESTAMP_COL]
            prev_date = cannot_handle[1]
            # How long has the ship been stopped for?
            if (current_date - prev_date).seconds >= STOPPED_TIME_SECONDS_THRESHOLD:
                trajectory = _finalize_trajectory(
                    mmsi, trajectory_dataframe, from_idx, cannot_handle[0], infer_stopped=False
                )
                trajectories = _construct_stopped_trajectory(mmsi, trajectory_dataframe, cannot_handle[0])
                return pd.concat([trajectory, trajectories])

        if prev_row is not None and _constraint_time_difference(row, prev_row):
            trajectory = _finalize_trajectory(
                mmsi,
                trajectory_dataframe,
                from_idx,
                idx,
                infer_stopped=False
            )
            trajectories = _construct_moving_trajectory(mmsi, trajectory_dataframe, idx)
            return pd.concat([trajectory, trajectories])

        prev_row = row

    return _finalize_trajectory(mmsi, trajectory_dataframe, from_idx, len(trajectory_dataframe.index),
                                infer_stopped=False)


def _finalize_trajectory(mmsi: int, trajectory_dataframe: gpd.GeoDataFrame, from_idx: int, to_idx: int,
                         infer_stopped: bool) -> pd.DataFrame:
    """
    Construct a trajectory as a pandas dataframe from a given set of AIS points.

    Keyword arguements:
        mmsi: the maritime mobile mervice identity used to identify a ship
        trajectory_dataframe: geopandas dataframe containing AIS points for a single ship
        from_idx: the index to start creating trajectories from (inclusive)
        to_idx: the index to stop creating trajectories at (exclusive)
    """
    to_idx -= 1  # to_idx is exclusive
    dataframe = _create_trajectory_db_df()
    # If there is no point in a trajectory which contain less points than in threshold, return empty dataframe
    if (to_idx < from_idx) or ((to_idx - from_idx + 1) <= POINTS_FOR_TRAJECTORY_THRESHOLD):
        return dataframe

    working_dataframe = trajectory_dataframe.truncate(before=from_idx, after=to_idx)

    trajectory = _convert_dataframe_to_trajectory(working_dataframe)
    start_datetime = trajectory_dataframe.iloc[from_idx][TIMESTAMP_COL]
    end_datetime = trajectory_dataframe.iloc[to_idx][TIMESTAMP_COL]

    # Groupby: eta, nav_status, draught, destination
    column_subset = [ETA_COL, NAVIGATIONAL_STATUS_COL, DESTINATION_COL]
    sorted_series_by_frequency = _find_most_recurring(working_dataframe, column_subset=column_subset, drop_na=False)
    eta = sorted_series_by_frequency[ETA_COL][0]
    nav_status = sorted_series_by_frequency[NAVIGATIONAL_STATUS_COL][0]
    destination = sorted_series_by_frequency[DESTINATION_COL][0]

    # Split eta, start_datetime, and end_datetime and create their smart keys
    eta_date_id = extract_smart_date_id_from_date(eta)
    eta_time_id = _extract_time_smart_id(eta)
    start_date_id = extract_smart_date_id_from_date(start_datetime)
    start_time_id = _extract_time_smart_id(start_datetime)
    end_date_id = extract_smart_date_id_from_date(end_datetime)
    end_time_id = _extract_time_smart_id(end_datetime)

    duration = end_datetime - start_datetime
    rot = _tfloat_from_dataframe(working_dataframe, ROT_COL)
    heading = _tfloat_from_dataframe(working_dataframe, HEADING_COL)
    draught = _tfloat_from_dataframe(working_dataframe, DRAUGHT_COL)

    # Ship information
    # Change 'Unknown' and 'Undefined' to NaN values, so they can be disregarded
    trajectory_dataframe.replace(['Unknown', 'Undefined'], pd.NA, inplace=True)
    # Find the most recurring information for the different AIS message attributes
    most_recurring = \
        _find_most_recurring(dataframe=trajectory_dataframe, column_subset=[IMO_COL], drop_na=True)
    imo = most_recurring[IMO_COL].iloc[0] if most_recurring.size != 0 else UNKNOWN_INT_VALUE

    most_recurring = _find_most_recurring(dataframe=trajectory_dataframe, column_subset=[MOBILE_TYPE_COL],
                                          drop_na=True)
    mobile_type = \
        most_recurring[MOBILE_TYPE_COL].iloc[0] if most_recurring.size != 0 else UNKNOWN_STRING_VALUE

    most_recurring = _find_most_recurring(dataframe=trajectory_dataframe, column_subset=[POSITION_FIXING_DEVICE_COL],
                                          drop_na=True)
    position_device = \
        most_recurring[POSITION_FIXING_DEVICE_COL].iloc[0] if most_recurring.size != 0 else UNKNOWN_STRING_VALUE

    most_recurring = _find_most_recurring(dataframe=trajectory_dataframe, column_subset=[SHIP_TYPE_COL], drop_na=True)
    ship_type = most_recurring[SHIP_TYPE_COL].iloc[0] if most_recurring.size != 0 else UNKNOWN_STRING_VALUE

    most_recurring = _find_most_recurring(dataframe=trajectory_dataframe, column_subset=[NAME_COL], drop_na=True)
    ship_name = most_recurring[NAME_COL].iloc[0] if most_recurring.size != 0 else UNKNOWN_STRING_VALUE

    most_recurring = \
        _find_most_recurring(dataframe=trajectory_dataframe, column_subset=[CALLSIGN_COL], drop_na=True)
    ship_callsign = most_recurring[CALLSIGN_COL].iloc[0] if most_recurring.size != 0 else UNKNOWN_STRING_VALUE

    most_recurring = _find_most_recurring(
        dataframe=trajectory_dataframe, column_subset=[LOCATION_SYSTEM_TYPE_COL], drop_na=True)
    location_system_type = most_recurring[LOCATION_SYSTEM_TYPE_COL].iloc[0] \
        if most_recurring.size != 0 else UNKNOWN_STRING_VALUE

    most_recurring = _find_most_recurring(dataframe=trajectory_dataframe, column_subset=[A_COL], drop_na=True)
    a = most_recurring[A_COL].iloc[0] if most_recurring.size != 0 else UNKNOWN_FLOAT_VALUE

    most_recurring = _find_most_recurring(dataframe=trajectory_dataframe, column_subset=[B_COL], drop_na=True)
    b = most_recurring[B_COL].iloc[0] if most_recurring.size != 0 else UNKNOWN_FLOAT_VALUE

    most_recurring = _find_most_recurring(dataframe=trajectory_dataframe, column_subset=[C_COL], drop_na=True)
    c = most_recurring[C_COL].iloc[0] if most_recurring.size != 0 else UNKNOWN_FLOAT_VALUE

    most_recurring = _find_most_recurring(dataframe=trajectory_dataframe, column_subset=[D_COL], drop_na=True)
    d = most_recurring[D_COL].iloc[0] if most_recurring.size != 0 else UNKNOWN_FLOAT_VALUE

    return pd.concat([dataframe, _create_trajectory_db_df(dict={
        T_START_DATE_COL: start_date_id,
        T_START_TIME_COL: start_time_id,
        T_END_DATE_COL: end_date_id,
        T_END_TIME_COL: end_time_id,
        T_ETA_DATE_COL: eta_date_id,
        T_ETA_TIME_COL: eta_time_id,
        T_NAVIGATIONAL_STATUS_COL: nav_status,
        # Measures
        T_DURATION_COL: duration,
        T_TRAJECTORY_COL: trajectory,
        T_INFER_STOPPED_COL: infer_stopped,
        T_DESTINATION_COL: destination,
        T_ROT_COL: rot,
        T_HEADING_COL: heading,
        T_DRAUGHT_COL: draught,
        # Ship
        T_IMO_COL: imo,
        T_MMSI_COL: mmsi,
        T_MOBILE_TYPE_COL: mobile_type,
        T_SHIP_TYPE_COL: ship_type,
        T_SHIP_NAME_COL: ship_name,
        T_SHIP_CALLSIGN_COL: ship_callsign,
        T_LOCATION_SYSTEM_TYPE_COL: location_system_type,
        T_POSITION_FIXING_DEVICE_COL: position_device,
        T_A_COL: a,
        T_B_COL: b,
        T_C_COL: c,
        T_D_COL: d,
    })])


def _extract_time_smart_id(datetime: datetime) -> int:
    """
    Extract the time integer smart-key from a given datetime.

    Keyword arguments:
        datetime: object representation of a datetime to extract time smart-key from
    """
    if pd.isna(datetime):
        return UNKNOWN_INT_VALUE
    return (datetime.hour * 10000) + (datetime.minute * 100) + datetime.second


def _tfloat_from_dataframe(dataframe: gpd.GeoDataFrame, float_column: str, remove_nan: bool = True) -> TFloatSeq | None:
    """
    Convert a geodataframe float64 column's values to a MobilityDB temporal float instant set.

    Keyword arguments:
        dataframe: geodataframe containing the float_column and timestamps
        float_column: name of the column containing the float values
        remove_nan: ensure NaN-values are not added to the tfloat (default: True)
    """
    if remove_nan:
        dataframe = dataframe.dropna(axis='index', subset=[float_column])

    if dataframe.empty:
        return None

    # Remove sequential duplicates, i.e. the values 1, 1, 1, 2, 1, 1 will become 1, 2, 1.
    df = dataframe[dataframe[float_column] != dataframe[float_column].shift()]

    # Add the last row to the dataframe if the timestamp is not the same as the last row.
    last_row = dataframe.iloc[[-1]]
    if not last_row[TIMESTAMP_COL].equals(df.iloc[[-1]][TIMESTAMP_COL]):
        df = pd.concat([df, last_row])

    # Make a new series of TFloatInsts based on the float and timestamp column.
    series = df.apply(
        lambda row: TFloatInst(str(row[float_column]) + '@' + row[TIMESTAMP_COL].strftime(MOBILITYDB_TIMESTAMP_FORMAT)),
        axis=1

    )
    return TFloatSeq(series.tolist(), lower_inc=True, upper_inc=True, interp='Stepwise')


def _find_most_recurring(dataframe: gpd.GeoDataFrame, column_subset: List[str], drop_na: bool) -> pd.Series:
    """
    Create and return a pandas series containing the values in descending order of occurrence.

    Keyword arguments:
        dataframe: dataframe containing the data to search through
        column_subset: list of column names that to find most recurring values for
        drop_na: indicates if pandas or numpy NA values should be included
    """
    return dataframe.value_counts(subset=column_subset, sort=True, dropna=drop_na).index.to_frame()


def _construct_stopped_trajectory(mmsi: int, trajectory_dataframe: gpd.GeoDataFrame, from_idx: int) -> pd.DataFrame:
    """
    Construct and returns trajectories as a pandas dataframe from the AIS data.

    Keyword arguments:
        mmsi: the maritime mobile mervice identity used to identify a ship
        trajectory_dataframe: geopandas dataframe containing AIS points for a single ship
        from_idx: the index to start creating trajectories from
    """
    prev_row = None
    for idx, row in trajectory_dataframe.iloc[from_idx:].iterrows():
        if row[SOG_COL] >= STOPPED_KNOTS_THRESHOLD:
            stopped_trajectory = _finalize_trajectory(mmsi, trajectory_dataframe, from_idx, idx, infer_stopped=True)
            trajectories = _construct_moving_trajectory(mmsi, trajectory_dataframe, idx)
            return pd.concat([stopped_trajectory, trajectories])

        if prev_row is not None and _constraint_time_difference(row, prev_row):
            trajectory = _finalize_trajectory(
                    mmsi,
                    trajectory_dataframe,
                    from_idx,
                    idx,
                    infer_stopped=True
                )
            trajectories = _construct_stopped_trajectory(mmsi, trajectory_dataframe, idx)
            return pd.concat([trajectory, trajectories])

        # Update the previous row
        prev_row = row

    stopped_trajectory = _finalize_trajectory(mmsi, trajectory_dataframe, from_idx, len(trajectory_dataframe.index),
                                              infer_stopped=True)
    return stopped_trajectory


def _convert_dataframe_to_trajectory(trajectory_dataframe: pd.DataFrame) -> TGeomPointSeq:
    """
    Create MobilityDB trajectory representation from a trajectory dataframe.

    Keyword arguments:
        trajectory_dataframe: dataframe containing trajectory data
    """
    mobilitydb_dataframe = pd.DataFrame(columns=[MBDB_TRAJECTORY_COL])
    mobilitydb_dataframe[TIMESTAMP_COL] = trajectory_dataframe[TIMESTAMP_COL].apply(
        func=lambda t: t.strftime(MOBILITYDB_TIMESTAMP_FORMAT))
    mobilitydb_dataframe[MBDB_TRAJECTORY_COL] = \
        trajectory_dataframe[GEO_PANDAS_GEOMETRY_COL].astype(str) + '@' + mobilitydb_dataframe[TIMESTAMP_COL]

    mobility_str = f"[{','.join(mobilitydb_dataframe[MBDB_TRAJECTORY_COL])}]"

    return TGeomPointSeq(mobility_str, srid=TRAJECTORY_SRID)


def rebuild_to_geodataframe(pandas_dataframe: pd.DataFrame) -> gpd.GeoDataFrame:
    """
    Rebuild a geodataframe from a pandas dataframe.

    Keyword arguements:
        pandas_dataframe: pandas dataframe with an existing 'geomtry' column
    """
    if GEO_PANDAS_GEOMETRY_COL in pandas_dataframe.columns:
        pandas_dataframe.drop(labels=GEO_PANDAS_GEOMETRY_COL, axis='columns', inplace=True)
    return gpd.GeoDataFrame(data=pandas_dataframe, geometry=gpd.points_from_xy(x=pandas_dataframe[LONGITUDE_COL],
                                                                               y=pandas_dataframe[LATITUDE_COL],
                                                                               crs=COORDINATE_REFERENCE_SYSTEM))


def _remove_outliers(dataframe: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Detect and remove outliers from geopandas geodataframe.

    Keyword arguements:
        dataframe: dataframe containing sorted AIS data points
    """
    prev_row: Optional[pd.Series] = None
    dataframe = dataframe.to_crs(COORDINATE_REFERENCE_SYSTEM_METERS)
    dataframe['is_outlier'] = False

    for (idx, row) in dataframe.iterrows():
        if prev_row is None:  # this is the first point
            prev_row = (idx, row)
            continue

        if not _check_outlier(dataframe, cur_point=(idx, row), prev_point=prev_row,
                              speed_threshold=SPEED_THRESHOLD_KNOTS, dist_func=_euclidian_dist):
            prev_row = (idx, row)
            continue
        # set as outlier in df
        dataframe.at[idx, 'is_outlier'] = True

    # remove outliers
    dataframe = dataframe[dataframe['is_outlier'] == False]  # noqa: E712
    dataframe.drop(labels='is_outlier', axis='columns', inplace=True)

    return dataframe.to_crs(COORDINATE_REFERENCE_SYSTEM)


def _check_outlier(dataframe: gpd.GeoDataFrame, cur_point: Tuple[int, gpd.GeoSeries],
                   prev_point: Tuple[int, gpd.GeoSeries],
                   speed_threshold: float, dist_func: Callable[[float, float, float, float], float]) -> bool:
    """
    Check whether the current point is an outlier.

    Keyword arguments:
        dataframe: the dataframe containing curr AIS point that is checked. Used for updating in-place.
        cur_point: A tuple consisting of the index of current point and a series representing current point.
        prev_point: A tuple consisting of the index of previous point and a series representing previous point.
        speed_threshold: max speed that determine whether an AIS point is an outlier
        dist_func: distance function used to calculate the distance between cur_point and prev_point
    """
    time_delta_ns = cur_point[1][TIMESTAMP_COL] - prev_point[1][TIMESTAMP_COL]
    # To get time_delta in seconds, divide by 1s in nano seconds.
    time_delta = time_delta_ns / np.timedelta64(1, 's')
    # Previous and current point is in the same timestamp, detect it as an outlier
    if time_delta == 0:
        return True

    cur_geom = cur_point[1][GEO_PANDAS_GEOMETRY_COL]
    prev_point_geom = prev_point[1][GEO_PANDAS_GEOMETRY_COL]

    distance = dist_func(cur_geom.x, cur_geom.y, prev_point_geom.x, prev_point_geom.y)
    computed_speed = distance / time_delta  # m/s
    speed = computed_speed * KNOTS_PER_METER_SECONDS
    sog = cur_point[1][SOG_COL]
    speed_to_determine_outlier = sog  # Default to sog

    # if SOG is nan, or the delta between calculated and sog is above the threshold, replace it with calculated speed.
    if np.isnan(sog) or abs(sog - speed) >= COMPUTED_VS_SOG_KNOTS_THRESHOLD:
        dataframe.at[cur_point[0], SOG_COL] = speed
        speed_to_determine_outlier = speed

    return speed_to_determine_outlier > speed_threshold


def _euclidian_dist(a_long: float, a_lat: float, b_long: float, b_lat: float) -> float:
    """
    Calculate the euclidean distance between 2 points.

    Keyword arguments:
        a_long: longitude value for point a
        a_lat: latitude value for point a
        b_long: longitude value for point b
        b_lat: latitude value for point b
    """
    return math.sqrt(
        (math.pow((b_long - a_long), 2) + math.pow((b_lat - a_lat), 2))
    )


def _create_trajectory_db_df(dict=None) -> pd.DataFrame:
    """
    Create trajectory dataframe representing DWH structure.

    Keyword arguments:
        dict: a dictionary with initial values for the created dataframe (default {})
    """
    if dict is None:
        dict = {}

    return pd.DataFrame({
        # Dimensions
        T_START_DATE_COL: pd.Series(dtype='int64', data=dict[T_START_DATE_COL] if T_START_DATE_COL in dict else []),
        T_START_TIME_COL: pd.Series(dtype='int64', data=dict[T_START_TIME_COL] if T_START_TIME_COL in dict else []),
        T_END_DATE_COL: pd.Series(dtype='int64', data=dict[T_END_DATE_COL] if T_END_DATE_COL in dict else []),
        T_END_TIME_COL: pd.Series(dtype='int64', data=dict[T_END_TIME_COL] if T_END_TIME_COL in dict else []),
        T_ETA_DATE_COL: pd.Series(dtype='int64', data=dict[T_ETA_DATE_COL] if T_ETA_DATE_COL in dict else []),
        T_ETA_TIME_COL: pd.Series(dtype='int64', data=dict[T_ETA_TIME_COL] if T_ETA_TIME_COL in dict else []),
        T_NAVIGATIONAL_STATUS_COL: pd.Series(dtype='object', data=dict[
            T_NAVIGATIONAL_STATUS_COL] if T_NAVIGATIONAL_STATUS_COL in dict else []),
        # Measures
        T_DURATION_COL: pd.Series(dtype='timedelta64[ns]', data=dict[T_DURATION_COL] if T_DURATION_COL in dict else []),
        T_TRAJECTORY_COL: pd.Series(dtype='object', data=dict[T_TRAJECTORY_COL] if T_TRAJECTORY_COL in dict else []),
        T_INFER_STOPPED_COL: pd.Series(dtype='bool',
                                       data=dict[T_INFER_STOPPED_COL] if T_INFER_STOPPED_COL in dict else []),
        T_DESTINATION_COL: pd.Series(dtype='object', data=dict[T_DESTINATION_COL] if T_DESTINATION_COL in dict else []),
        T_ROT_COL: pd.Series(dtype='object', data=dict[T_ROT_COL] if T_ROT_COL in dict else []),
        T_HEADING_COL: pd.Series(dtype='object', data=dict[T_HEADING_COL] if T_HEADING_COL in dict else []),
        T_DRAUGHT_COL: pd.Series(dtype='object', data=dict[T_DRAUGHT_COL] if T_DRAUGHT_COL in dict else []),
        # Ship
        T_IMO_COL: pd.Series(dtype='int64', data=dict[T_IMO_COL] if T_IMO_COL in dict else []),
        T_MMSI_COL: pd.Series(dtype='int64', data=dict[T_MMSI_COL] if T_MMSI_COL in dict else []),
        T_MOBILE_TYPE_COL: pd.Series(dtype='object', data=dict[T_MOBILE_TYPE_COL] if T_MOBILE_TYPE_COL in dict else []),
        T_POSITION_FIXING_DEVICE_COL: pd.Series(dtype='object', data=dict[T_POSITION_FIXING_DEVICE_COL] \
                                                if T_POSITION_FIXING_DEVICE_COL in dict else []),
        T_SHIP_TYPE_COL: pd.Series(dtype='object', data=dict[T_SHIP_TYPE_COL] if T_SHIP_TYPE_COL in dict else []),
        T_SHIP_NAME_COL: pd.Series(dtype='object', data=dict[T_SHIP_NAME_COL] if T_SHIP_NAME_COL in dict else []),
        T_SHIP_CALLSIGN_COL: pd.Series(dtype='object',
                                       data=dict[T_SHIP_CALLSIGN_COL] if T_SHIP_CALLSIGN_COL in dict else []),
        T_LOCATION_SYSTEM_TYPE_COL: pd.Series(
            dtype='object', data=dict[T_LOCATION_SYSTEM_TYPE_COL] \
            if T_LOCATION_SYSTEM_TYPE_COL in dict else []
        ),
        T_A_COL: pd.Series(dtype='float64', data=dict[T_A_COL] if T_A_COL in dict else []),
        T_B_COL: pd.Series(dtype='float64', data=dict[T_B_COL] if T_B_COL in dict else []),
        T_C_COL: pd.Series(dtype='float64', data=dict[T_C_COL] if T_C_COL in dict else []),
        T_D_COL: pd.Series(dtype='float64', data=dict[T_D_COL] if T_D_COL in dict else []),
    })
