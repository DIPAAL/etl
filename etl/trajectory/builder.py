"""Module handling trajectory construction and outlier detection."""
from concurrent.futures import ProcessPoolExecutor
import geopandas as gpd
import pandas as pd
import math
from datetime import datetime
from mobilitydb import TGeomPointSeq, TFloatInstSet, TFloatInst
from typing import List
from etl.constants import COORDINATE_REFERENCE_SYSTEM, LONGITUDE_COL, LATITUDE_COL, TIMESTAMP_COL, SOG_COL, MMSI_COL, \
    ETA_COL, DESTINATION_COL, NAVIGATIONAL_STATUS_COL, DRAUGHT_COL, ROT_COL, HEADING_COL, IMO_COL, \
    POSITION_FIXING_DEVICE_COL, SHIP_TYPE_COL, NAME_COL, CALLSIGN_COL, A_COL, B_COL, C_COL, D_COL, \
    MBDB_TRAJECTORY_COL, GEO_PANDAS_GEOMETRY_COL, LOCATION_SYSTEM_TYPE_COL, T_LOCATION_SYSTEM_TYPE_COL, T_LENGTH_COL, \
    TRAJECTORY_SRID, ASSUMED_SPEED_COL, CALCULATED_SPEED_COL, TEMPORAL_DISTANCE_COL, SPATIAL_DISTANCE_COL, \
    IS_OUTLIER_COL, INTERLACED_SPEED_COL
from etl.constants import T_INFER_STOPPED_COL, T_DURATION_COL, T_C_COL, T_D_COL, T_TRAJECTORY_COL, T_DESTINATION_COL, \
    T_ROT_COL, T_HEADING_COL, T_MMSI_COL, T_IMO_COL, T_B_COL, T_A_COL, T_MOBILE_TYPE_COL, T_SHIP_TYPE_COL, \
    T_SHIP_NAME_COL, T_SHIP_CALLSIGN_COL, T_NAVIGATIONAL_STATUS_COL, T_DRAUGHT_COL, T_ETA_TIME_COL, T_ETA_DATE_COL, \
    T_START_TIME_COL, T_START_DATE_COL, T_END_TIME_COL, T_END_DATE_COL
from tqdm import tqdm

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
UNKNOWN_INT_VALUE = -1
UNKNOWN_FLOAT_VALUE = -1.0


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


def _construct_moving_trajectory(mmsi: int, trajectory_dataframe: gpd.GeoDataFrame, from_idx: int) -> pd.DataFrame:
    """
    Construct and returns trajectories from the AIS data as a pandas dataframe.

    Keyword arguments:
        mmsi: the maritime mobile mervice identity used to identify a ship
        trajectory_dataframe: geopandas dataframe containing AIS points for a single ship
        from_idx: the index to start creating trajectories from
    """
    idx_cannot_handle = None
    for idx in range(from_idx, len(trajectory_dataframe.index)):
        row = trajectory_dataframe.iloc[idx]

        # Has the ship possibly stopped?
        if row[ASSUMED_SPEED_COL] < STOPPED_KNOTS_THRESHOLD:
            # Have we already detected a possible stop?
            if idx_cannot_handle is not None:
                current_date = row[TIMESTAMP_COL]
                prev_date = trajectory_dataframe.iloc[idx_cannot_handle][TIMESTAMP_COL]
                # How long has the ship been stopped for?
                if (current_date - prev_date).seconds >= STOPPED_TIME_SECONDS_THRESHOLD:
                    trajectory = _finalize_trajectory(
                        mmsi, trajectory_dataframe, from_idx, idx_cannot_handle, infer_stopped=False
                    )
                    trajectories = _construct_stopped_trajectory(mmsi, trajectory_dataframe, idx_cannot_handle)
                    return pd.concat([trajectory, trajectories])
            else:
                # We have a possible stop
                idx_cannot_handle = idx
        else:
            # Reset any possible stop because we are currently moving
            idx_cannot_handle = None

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
    eta_date_id = extract_date_smart_id(eta)
    eta_time_id = _extract_time_smart_id(eta)
    start_date_id = extract_date_smart_id(start_datetime)
    start_time_id = _extract_time_smart_id(start_datetime)
    end_date_id = extract_date_smart_id(end_datetime)
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

    most_recurring = _find_most_recurring(dataframe=trajectory_dataframe, column_subset=[POSITION_FIXING_DEVICE_COL],
                                          drop_na=True)
    mobile_type = \
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

    # Metadata
    # The total delta length of all points in the trajectory
    total_length = _calculate_delta_length(working_dataframe)

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
        T_A_COL: a,
        T_B_COL: b,
        T_C_COL: c,
        T_D_COL: d,
        # Metadata
        T_LENGTH_COL: total_length
    })])


def extract_date_smart_id(datetime: datetime) -> int:
    """
    Extract the date integer smart-key from a given datetime.

    Keyword arguments:
        datetime: object representation of a datetime to extract date smart-key from
    """
    if pd.isna(datetime):
        return UNKNOWN_INT_VALUE
    return (datetime.year * 10000) + (datetime.month * 100) + datetime.day


def _extract_time_smart_id(datetime: datetime) -> int:
    """
    Extract the time integer smart-key from a given datetime.

    Keyword arguments:
        datetime: object representation of a datetime to extract time smart-key from
    """
    if pd.isna(datetime):
        return UNKNOWN_INT_VALUE
    return (datetime.hour * 10000) + (datetime.minute * 100) + datetime.second


def _tfloat_from_dataframe(dataframe: gpd.GeoDataFrame, float_column: str, remove_nan: bool = True) -> TFloatInstSet:
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

    tfloat_lst = []
    for _, row in dataframe.iterrows():
        mobilitydb_timestamp = row[TIMESTAMP_COL].strftime(MOBILITYDB_TIMESTAMP_FORMAT)
        float_val = str(row[float_column])
        tfloat_lst.append(TFloatInst(str(float_val + '@' + mobilitydb_timestamp)))

    return TFloatInstSet(*tfloat_lst)


def _find_most_recurring(dataframe: gpd.GeoDataFrame, column_subset: List[str], drop_na: bool) -> pd.Series:
    """
    Create and return a pandas series containing the values in descending order of occurrence.

    Keyword arguments:
        dataframe: dataframe containing the data to search through
        column_subset: list of column names that to find most recurring values for
        drop_na: indicates if pandas or numpy NA values should be included
    """
    return dataframe.value_counts(subset=column_subset, sort=True, dropna=drop_na).index.to_frame()


def _calculate_delta_length(dataframe: gpd.GeoDataFrame) -> float:
    """
    Calculate the total delta length of all points in the trajectory.

    Keyword arguments:
        dataframe: dataframe containing the data to calculate delta length for
    """
    # Extract geometry column as a GeoSeries with CRS for meters
    point_gs = dataframe[GEO_PANDAS_GEOMETRY_COL].to_crs(COORDINATE_REFERENCE_SYSTEM_METERS)
    return point_gs.distance(point_gs.shift(1)).sum()


def _construct_stopped_trajectory(mmsi: int, trajectory_dataframe: gpd.GeoDataFrame, from_idx: int) -> pd.DataFrame:
    """
    Construct and returns trajectories as a pandas dataframe from the AIS data.

    Keyword arguments:
        mmsi: the maritime mobile mervice identity used to identify a ship
        trajectory_dataframe: geopandas dataframe containing AIS points for a single ship
        from_idx: the index to start creating trajectories from
    """
    for idx in range(from_idx, len(trajectory_dataframe.index)):
        row = trajectory_dataframe.iloc[idx]

        if row[ASSUMED_SPEED_COL] >= STOPPED_KNOTS_THRESHOLD:
            stopped_trajectory = _finalize_trajectory(mmsi, trajectory_dataframe, from_idx, idx, infer_stopped=True)
            trajectories = _construct_moving_trajectory(mmsi, trajectory_dataframe, idx)
            return pd.concat([stopped_trajectory, trajectories])

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


def recalculate_point(dataframe, current_index, previous_index):
    previous_row = dataframe.iloc[previous_index]
    current_row = dataframe.iloc[current_index]

    dataframe.loc[current_index, SPATIAL_DISTANCE_COL] = previous_row.geometry.distance(current_row.geometry)
    dataframe.loc[current_index, TEMPORAL_DISTANCE_COL] = (
                current_row[TIMESTAMP_COL] - previous_row[TIMESTAMP_COL]).total_seconds()
    # if temporal distance is 0, then set outlier to true and return
    if dataframe.loc[current_index, TEMPORAL_DISTANCE_COL] == 0:
        dataframe.loc[current_index, IS_OUTLIER_COL] = True
        return
    dataframe.loc[current_index, CALCULATED_SPEED_COL] = dataframe.loc[current_index, SPATIAL_DISTANCE_COL] / \
                                                             dataframe.loc[
                                                                 current_index, TEMPORAL_DISTANCE_COL] * KNOTS_PER_METER_SECONDS
    dataframe.loc[current_index, INTERLACED_SPEED_COL] = current_row[SOG_COL] if not math.isnan(current_row[SOG_COL]) else \
    dataframe.loc[current_index, CALCULATED_SPEED_COL]
    dataframe.loc[current_index, ASSUMED_SPEED_COL] = \
        dataframe.loc[current_index, INTERLACED_SPEED_COL] \
            if abs(
            dataframe.loc[current_index, CALCULATED_SPEED_COL] -
            dataframe.loc[current_index, INTERLACED_SPEED_COL]
        ) <= COMPUTED_VS_SOG_KNOTS_THRESHOLD else dataframe.loc[current_index, CALCULATED_SPEED_COL]

    is_outlier = dataframe.loc[current_index, ASSUMED_SPEED_COL] > SPEED_THRESHOLD_KNOTS
    dataframe.loc[current_index, IS_OUTLIER_COL] = is_outlier
    return is_outlier

def _remove_detected_outliers(dataframe):
    """
    The dataframe input has determined if it contains outlier. Now we need to visit each outlier and remove it.

    Keyword arguments:
        dataframe: dataframe containing sorted AIS data points with outlier column.
    """

    while dataframe[IS_OUTLIER_COL].any():
        # Find the first outlier, which is always a true outlier.
        outlier_index = dataframe[dataframe[IS_OUTLIER_COL]].index[0]

        # Verify if it is still an outlier
        if not recalculate_point(dataframe, outlier_index, outlier_index-1):
            continue

        # Recalculate the next point based on the previous point, as this point is an outlier.
        _recalculate_next_point(dataframe, outlier_index)
        # Remove the outlier
        dataframe.drop(outlier_index, inplace=True)


def _recalculate_next_point(dataframe, outlier_index):
    # determine if next point exists
    if outlier_index + 1 >= len(dataframe.index):
        return

    next_row = dataframe.iloc[outlier_index + 1]

    # determine if previous point is an outlier
    if outlier_index == 0:
        # Recalculate based on calc_speed being nan, i.e. use sog
        speed = next_row[SOG_COL]
        dataframe.loc[outlier_index+1, IS_OUTLIER_COL] = speed > SPEED_THRESHOLD_KNOTS
    else:
        # recalculate spatial diff, temporal diff, calculated speed, interlaced speed and assumed_speed and detect if outlier.

        recalculate_point(dataframe, outlier_index+1, outlier_index-1)

def _remove_outliers(dataframe: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Detect and remove outliers from geopandas geodataframe.

    Keyword arguements:
        dataframe: dataframe containing sorted AIS data points
    """
    dataframe = dataframe.to_crs(COORDINATE_REFERENCE_SYSTEM_METERS)

    dataframe = _assign_speed_to_df(dataframe)

    # Make a column 'is_outlier' that is true if
    # 1. Time_diff is 0 or
    # 3. assumed speed is greater than speed threshold.
    dataframe.loc[:, IS_OUTLIER_COL] = dataframe.apply(
        lambda row:
        row[TEMPORAL_DISTANCE_COL] == 0 or
        row[ASSUMED_SPEED_COL] > SPEED_THRESHOLD_KNOTS,
        axis=1
    )

    # Remove outliers
    _remove_detected_outliers(dataframe)

    # As some of the calculated speeds are outliers, we need to recalculate the speeds
    dataframe = _assign_speed_to_df(dataframe)

    return dataframe.to_crs(COORDINATE_REFERENCE_SYSTEM)

def _assign_speed_to_df(dataframe):
    # Calculate the diff between points in meters
    dataframe.loc[:, SPATIAL_DISTANCE_COL] = dataframe.geometry.distance(dataframe.geometry.shift(1))

    # Calculate the time diff between points in seconds
    dataframe.loc[:, TEMPORAL_DISTANCE_COL] = dataframe[TIMESTAMP_COL].diff().dt.total_seconds()

    # Calculate the speed between points in knots
    dataframe.loc[:, CALCULATED_SPEED_COL] = dataframe[SPATIAL_DISTANCE_COL] / dataframe[
        TEMPORAL_DISTANCE_COL] * KNOTS_PER_METER_SECONDS

    # Make a column 'interlaced_speed' which is calc_speed if sog is nan, otherwise sog
    dataframe.loc[:, INTERLACED_SPEED_COL] = dataframe.apply(
        lambda row: row[SOG_COL] if not math.isnan(row[SOG_COL]) else row[CALCULATED_SPEED_COL], axis=1)

    # Make a column 'assumed_speed' which is calc_speed if the difference between calc_speed,
    # and interlaced_speed is less than COMPUTED_VS_SOG_KNOTS_THRESHOLD, otherwise interlaced_speed
    dataframe.loc[:, ASSUMED_SPEED_COL] = dataframe.apply(
        lambda row: row[INTERLACED_SPEED_COL] if abs(row[CALCULATED_SPEED_COL] - row[INTERLACED_SPEED_COL])
            <= COMPUTED_VS_SOG_KNOTS_THRESHOLD else row[CALCULATED_SPEED_COL],  # noqa: E131
        axis=1
    )

    return dataframe


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


def _create_trajectory_db_df(dict={}) -> pd.DataFrame:
    """
    Create trajectory dataframe representing DWH structure.

    Keyword arguments:
        dict: a dictionary with initial values for the created dataframe (default {})
    """
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
        T_SHIP_TYPE_COL: pd.Series(dtype='object', data=dict[T_SHIP_TYPE_COL] if T_SHIP_TYPE_COL in dict else []),
        T_SHIP_NAME_COL: pd.Series(dtype='object', data=dict[T_SHIP_NAME_COL] if T_SHIP_NAME_COL in dict else []),
        T_SHIP_CALLSIGN_COL: pd.Series(dtype='object',
                                       data=dict[T_SHIP_CALLSIGN_COL] if T_SHIP_CALLSIGN_COL in dict else []),
        T_LOCATION_SYSTEM_TYPE_COL: pd.Series(
            dtype='object', data=dict[T_LOCATION_SYSTEM_TYPE_COL] if T_LOCATION_SYSTEM_TYPE_COL in dict else []),
        T_A_COL: pd.Series(dtype='float64', data=dict[T_A_COL] if T_A_COL in dict else []),
        T_B_COL: pd.Series(dtype='float64', data=dict[T_B_COL] if T_B_COL in dict else []),
        T_C_COL: pd.Series(dtype='float64', data=dict[T_C_COL] if T_C_COL in dict else []),
        T_D_COL: pd.Series(dtype='float64', data=dict[T_D_COL] if T_D_COL in dict else []),
        # Metadata
        T_LENGTH_COL: pd.Series(dtype='float64', data=dict[T_LENGTH_COL] if T_LENGTH_COL in dict else []),
    })
