import geopandas as gpd
import pandas as pd
import math
from datetime import datetime
from mobilitydb import TGeomPointSeq, TFloatInstSet, TFloatInst
from typing import Callable, Optional, List
from etl.constants import COORDINATE_REFERENCE_SYSTEM, LONGITUDE_COL, LATITUDE_COL, TIMESTAMP_COL, SOG_COL, MMSI_COL, ETA_COL, DESTINATION_COL, NAVIGATIONAL_STATUS_COL, DRAUGHT_COL, ROT_COL, HEADING_COL, IMO_COL, POSITION_FIXING_DEVICE_COL, SHIP_TYPE_COL, NAME_COL, CALLSIGN_COL, A_COL, B_COL, C_COL, D_COL, MBDB_TRAJECTORY_COL, GEO_PANDAS_GEOMETRY_COL
from etl.constants import T_INFER_STOPPED_COL, T_DURATION_COL, T_C_COL, T_D_COL, T_TRAJECTORY_COL, T_DESTINATION_COL, T_ROT_COL, T_HEADING_COL, T_MMSI_COL, T_IMO_COL, T_B_COL, T_A_COL, T_MOBILE_TYPE_COL, T_SHIP_TYPE_COL, T_SHIP_NAME_COL, T_SHIP_CALLSIGN_COL, T_NAVIGATIONAL_STATUS_COL, T_DRAUGHT_COL, T_ETA_TIME_COL, T_ETA_DATE_COL, T_START_TIME_COL, T_START_DATE_COL, T_END_TIME_COL, T_END_DATE_COL

SPEED_THRESHOLD_KNOTS=100

MOBILITYDB_TIMESTAMP_FORMAT='%Y-%m-%d %H:%M:%S' #2020-01-01 00:00:00+01
COORDINATE_REFERENCE_SYSTEM_METERS='epsg:3034'
KNOTS_PER_METER_SECONDS=1.943844 # = 1 m/s
COMPUTED_VS_SOG_KNOTS_THRESHOLD=2
STOPPED_KNOTS_THRESHOLD=0.5
STOPPED_TIME_SECONDS_THRESHOLD=5*60 # 5 minutes
SPLIT_GAP_SECONDS_THRESHOLD=5*60 # 5 minutes
UNKNOWN_STRING_VALUE = 'Unknown'
UNKNOWN_INT_VALUE = -1
UNKNOWN_FLOAT_VALUE = -1.0

def build_from_geopandas(clean_sorted_ais: gpd.GeoDataFrame) -> pd.DataFrame:
    grouped_data = clean_sorted_ais.groupby(by=MMSI_COL)

    result_frames = []
    for mmsi, ship_data in grouped_data:
        ship_data.reset_index(inplace=True)
        result_frames.append(_create_trajectory(mmsi=mmsi, data=ship_data))

    return pd.concat(result_frames)

def _create_trajectory(mmsi: int, data: pd.DataFrame) -> pd.DataFrame:
    dataframe = _remove_outliers(dataframe=data)
    # Reset the index as some rows might have been classified as outliers and removed
    dataframe.reset_index(inplace=True)

    return _construct_moving_trajectory(mmsi, dataframe, 0)
        
def _construct_moving_trajectory(mmsi: int, trajectory_dataframe: gpd.GeoDataFrame, from_idx: int) -> pd.DataFrame:
    idx_cannot_handle = None
    for idx in range(from_idx, len(trajectory_dataframe.index)):
        row = trajectory_dataframe.iloc[idx]
        
        if row[SOG_COL] < STOPPED_KNOTS_THRESHOLD:
            if idx_cannot_handle is not None:
                current_date = row[TIMESTAMP_COL]
                if (current_date - trajectory_dataframe.iloc[idx_cannot_handle][TIMESTAMP_COL]).seconds >= STOPPED_TIME_SECONDS_THRESHOLD:
                    trajectory = _finalize_trajectory(mmsi, trajectory_dataframe, from_idx, idx_cannot_handle, infer_stopped=False)
                    trajectories = _construct_stopped_trajectory(mmsi, trajectory_dataframe, idx_cannot_handle)
                    return pd.concat([trajectory, trajectories])
            else:
                idx_cannot_handle = idx
        else:
            idx_cannot_handle = None
    
    return _finalize_trajectory(mmsi, trajectory_dataframe, from_idx, len(trajectory_dataframe.index), infer_stopped=False)

def _finalize_trajectory(mmsi: int, trajectory_dataframe: gpd.GeoDataFrame, from_idx: int, to_idx: int, infer_stopped: bool) -> pd.DataFrame:
    to_idx -= 1 # to_idx is exclusive
    dataframe = _create_trajectory_db_df()
    # In the case that there is no points in a trajectory, return empty dataframe
    if to_idx < from_idx:
        return dataframe

    working_dataframe = trajectory_dataframe.truncate(before=from_idx, after=to_idx)

    trajectory = _convert_dataframe_to_trajectory(working_dataframe)
    start_datetime = trajectory_dataframe.iloc[from_idx][TIMESTAMP_COL]
    end_datetime = trajectory_dataframe.iloc[to_idx][TIMESTAMP_COL]

    # Groupby: eta, nav_status, draught, destination
    column_subset = [ETA_COL, NAVIGATIONAL_STATUS_COL, DRAUGHT_COL, DESTINATION_COL]
    sorted_series_by_frequency = _find_most_recurring(working_dataframe, column_subset=column_subset, drop_na = False)
    eta = sorted_series_by_frequency[ETA_COL][0]
    nav_status = sorted_series_by_frequency[NAVIGATIONAL_STATUS_COL][0]
    draught = sorted_series_by_frequency[DRAUGHT_COL][0]
    destination = sorted_series_by_frequency[DESTINATION_COL][0]

    # Split eta, start_datetime, and end_datetime and create their smart keys
    eta_date_id = _extract_date_smart_id(eta)
    eta_time_id = _extract_time_smart_id(eta)
    start_date_id = _extract_date_smart_id(start_datetime)
    start_time_id = _extract_time_smart_id(start_datetime)
    end_date_id = _extract_date_smart_id(end_datetime)
    end_time_id = _extract_time_smart_id(end_datetime)

    duration = end_datetime - start_datetime
    rot = _tfloat_from_dataframe(working_dataframe, ROT_COL)
    heading = _tfloat_from_dataframe(working_dataframe, HEADING_COL)

    # Ship information
    # Change 'Unknown' and 'Undefined' to NaN values, so they can be disregarded
    trajectory_dataframe.replace(['Unknown', 'Undefined'], pd.NA, inplace=True)
    most_recurring = _find_most_recurring(dataframe=trajectory_dataframe, column_subset=[IMO_COL], drop_na=True)
    imo = most_recurring[IMO_COL].iloc[0] if most_recurring.size != 0 else UNKNOWN_INT_VALUE

    most_recurring = _find_most_recurring(dataframe=trajectory_dataframe, column_subset=[POSITION_FIXING_DEVICE_COL], drop_na=True)
    mobile_type = most_recurring[POSITION_FIXING_DEVICE_COL].iloc[0] if most_recurring.size != 0 else UNKNOWN_STRING_VALUE

    most_recurring = _find_most_recurring(dataframe=trajectory_dataframe, column_subset=[SHIP_TYPE_COL], drop_na=True)
    ship_type = most_recurring[SHIP_TYPE_COL].iloc[0] if most_recurring.size != 0 else UNKNOWN_STRING_VALUE

    most_recurring = _find_most_recurring(dataframe=trajectory_dataframe, column_subset=[NAME_COL], drop_na=True)
    ship_name = most_recurring[NAME_COL].iloc[0] if most_recurring.size != 0 else UNKNOWN_STRING_VALUE

    most_recurring = _find_most_recurring(dataframe=trajectory_dataframe, column_subset=[CALLSIGN_COL], drop_na=True)
    ship_callsign = most_recurring[CALLSIGN_COL].iloc[0] if most_recurring.size != 0 else UNKNOWN_STRING_VALUE

    most_recurring = _find_most_recurring(dataframe=trajectory_dataframe, column_subset=[A_COL], drop_na=True)
    a = most_recurring[A_COL].iloc[0] if most_recurring.size != 0 else UNKNOWN_FLOAT_VALUE

    most_recurring = _find_most_recurring(dataframe=trajectory_dataframe, column_subset=[B_COL], drop_na=True)
    b = most_recurring[B_COL].iloc[0] if most_recurring.size != 0 else UNKNOWN_FLOAT_VALUE

    most_recurring = _find_most_recurring(dataframe=trajectory_dataframe, column_subset=[C_COL], drop_na=True)
    c = most_recurring[C_COL].iloc[0] if most_recurring.size != 0 else UNKNOWN_FLOAT_VALUE

    most_recurring = _find_most_recurring(dataframe=trajectory_dataframe, column_subset=[D_COL], drop_na=True)
    d = most_recurring[D_COL].iloc[0] if most_recurring.size != 0 else UNKNOWN_FLOAT_VALUE


    return pd.concat([dataframe, pd.Series(data={
                                           T_START_DATE_COL: start_date_id, T_START_TIME_COL: start_time_id,
                                           T_END_DATE_COL: end_date_id, T_END_TIME_COL: end_time_id,
                                           T_ETA_DATE_COL: eta_date_id, T_ETA_TIME_COL: eta_time_id,
                                           T_NAVIGATIONAL_STATUS_COL: nav_status, T_DURATION_COL: duration,
                                           T_TRAJECTORY_COL: trajectory, T_INFER_STOPPED_COL: infer_stopped,
                                           T_DESTINATION_COL: destination, T_ROT_COL: rot,
                                           T_HEADING_COL: heading, T_DRAUGHT_COL: draught,
                                           T_MMSI_COL: mmsi, T_IMO_COL: imo,
                                           T_MOBILE_TYPE_COL: mobile_type, T_SHIP_TYPE_COL: ship_type,
                                           T_SHIP_NAME_COL: ship_name, T_SHIP_CALLSIGN_COL: ship_callsign,
                                           T_A_COL: a, T_B_COL: b, T_C_COL: c, T_D_COL: d
                                        }).to_frame().T])

def _extract_date_smart_id(datetime: datetime) -> int:
    return (datetime.year * 10000) + (datetime.month * 100) + (datetime.day)

def _extract_time_smart_id(datetime: datetime) -> int:
    return (datetime.hour * 10000) + (datetime.minute * 100) + (datetime.second)

def _tfloat_from_dataframe(dataframe: gpd.GeoDataFrame, float_column:str) -> TFloatInstSet:
    tfloat_lst = []
    for _, row in dataframe.iterrows():
        mobilitydb_timestamp = row[TIMESTAMP_COL].strftime(MOBILITYDB_TIMESTAMP_FORMAT) + '+01'
        float_val = str(row[float_column])
        tfloat_lst.append(TFloatInst(str(float_val + '@' + mobilitydb_timestamp)))
    
    return TFloatInstSet(*tfloat_lst)

def _find_most_recurring(dataframe: gpd.GeoDataFrame, column_subset: List[str], drop_na: bool) -> pd.Series:
    return dataframe.value_counts(subset=column_subset, sort=True, dropna=drop_na).index.to_frame()

def _construct_stopped_trajectory(mmsi: int, trajectory_dataframe: gpd.GeoDataFrame, from_idx: int) -> pd.DataFrame:
    for idx in range(from_idx, len(trajectory_dataframe.index)):
        row = trajectory_dataframe.iloc[idx]

        if row[SOG_COL] >= STOPPED_KNOTS_THRESHOLD:
            stopped_trajectory = _finalize_trajectory(mmsi, trajectory_dataframe, from_idx, idx, infer_stopped=True)
            trajectories = _construct_moving_trajectory(mmsi, trajectory_dataframe, idx)
            return pd.concat([stopped_trajectory, trajectories])

    stopped_trajectory = _finalize_trajectory(mmsi, trajectory_dataframe, from_idx, len(trajectory_dataframe.index), infer_stopped=True)
    return stopped_trajectory 

def _convert_dataframe_to_trajectory(trajectory_dataframe: pd.DataFrame) -> TGeomPointSeq:
    mobilitydb_dataframe = pd.DataFrame(columns=[MBDB_TRAJECTORY_COL])
    mobilitydb_dataframe[TIMESTAMP_COL] = trajectory_dataframe[TIMESTAMP_COL].apply(func=lambda t: t.strftime(MOBILITYDB_TIMESTAMP_FORMAT))
    mobilitydb_dataframe[MBDB_TRAJECTORY_COL] = trajectory_dataframe[GEO_PANDAS_GEOMETRY_COL].astype(str) + '@' + mobilitydb_dataframe[TIMESTAMP_COL]

    mobility_str = f"[{','.join(mobilitydb_dataframe[MBDB_TRAJECTORY_COL])}]"

    return TGeomPointSeq(mobility_str)

    
def _rebuild_to_geodataframe(pandas_dataframe: pd.DataFrame) -> gpd.GeoDataFrame:
    if GEO_PANDAS_GEOMETRY_COL in pandas_dataframe.columns:
        pandas_dataframe.drop(labels=GEO_PANDAS_GEOMETRY_COL, axis='columns', inplace=True)
    return gpd.GeoDataFrame(data=pandas_dataframe, geometry=gpd.points_from_xy(x=pandas_dataframe[LONGITUDE_COL], y=pandas_dataframe[LATITUDE_COL], crs=COORDINATE_REFERENCE_SYSTEM))


def _remove_outliers(dataframe: pd.DataFrame) -> gpd.GeoDataFrame:
    prev_row: Optional[pd.Series] = None
    result_dataframe = pd.DataFrame(columns=dataframe.columns)

    for idx in range(0, len(dataframe.index)):
        row = dataframe.iloc[idx]

        if prev_row is None: # this is the first point
            prev_row = row
            result_dataframe = pd.concat([result_dataframe, row.to_frame().T])
            continue


        if not _check_outlier(cur_point=row, prev_point=prev_row, speed_threshold=SPEED_THRESHOLD_KNOTS, dist_func=_euclidian_dist):
            prev_row = row
            result_dataframe = pd.concat([result_dataframe, row.to_frame().T])

    return _rebuild_to_geodataframe(result_dataframe)

def _check_outlier(cur_point: gpd.GeoSeries, prev_point: gpd.GeoSeries, speed_threshold: float, dist_func: Callable[[float, float, float, float], float]) -> bool:
    """
    Checks whether the distance between two points, given the provided distance function and threshold, is an outlier
        cur_point: Point as the geopandas row
        prev_point: Point as the geopandas row
        dist_threshold: Threshold distance which determines whether distance between the points indicates an outlier
        dist_function: A distance function that takes in 4 parameters (cur_long, cur_lat, prev_long, prev_lat) and returns the distance between the points

        Returns: A bool indicating that an outlier is detected
    """
    cur_point_converted_long_lat = cur_point.to_crs(crs=COORDINATE_REFERENCE_SYSTEM_METERS)[GEO_PANDAS_GEOMETRY_COL]
    prev_point_converted_long_lat = prev_point.to_crs(crs=COORDINATE_REFERENCE_SYSTEM_METERS)[GEO_PANDAS_GEOMETRY_COL]
    
    time_delta = cur_point[TIMESTAMP_COL] - prev_point[TIMESTAMP_COL]
    # Previous and current point is in the same timestamp, detect it as an outlier
    if time_delta.seconds == 0:
        return True

    distance = dist_func(cur_point_converted_long_lat.x, cur_point_converted_long_lat.y, prev_point_converted_long_lat.x, prev_point_converted_long_lat.y)
    
    computed_speed = distance/time_delta.seconds # m/s
    speed = computed_speed * KNOTS_PER_METER_SECONDS

    # The other group uses SOG if the absolute difference is above a threshold
    if abs((cur_point.get_sog() - speed) > COMPUTED_VS_SOG_KNOTS_THRESHOLD):
        speed = cur_point.get_sog()

    if speed > speed_threshold:
        return True
    return False

def _euclidian_dist(a_long:float, a_lat:float, b_long:float , b_lat:float) -> float:
    return math.sqrt(
        (math.pow((b_long - a_long), 2) + math.pow((b_lat - a_lat), 2))
    )

def _create_trajectory_db_df() -> pd.DataFrame:
    return pd.DataFrame({
        # Dimensions
        T_START_DATE_COL: pd.Series(dtype='int64'),
        T_START_TIME_COL: pd.Series(dtype='int64'),
        T_END_DATE_COL: pd.Series(dtype='int64'),
        T_END_TIME_COL: pd.Series(dtype='int64'),
        T_ETA_DATE_COL: pd.Series(dtype='int64'),
        T_ETA_TIME_COL: pd.Series(dtype='int64'),
        T_NAVIGATIONAL_STATUS_COL: pd.Series(dtype='object'),
        # Measures
        T_DURATION_COL: pd.Series(dtype='timedelta64[ns]'),
        T_TRAJECTORY_COL: pd.Series(dtype='object'),
        T_INFER_STOPPED_COL: pd.Series(dtype='bool'),
        T_DESTINATION_COL: pd.Series(dtype='object'),
        T_ROT_COL: pd.Series(dtype='object'),
        T_HEADING_COL: pd.Series(dtype='object'),
        T_DRAUGHT_COL: pd.Series(dtype='float64'),
        # Ship
        T_IMO_COL: pd.Series(dtype='int64'),
        T_MMSI_COL: pd.Series(dtype='int64'),
        T_MOBILE_TYPE_COL: pd.Series(dtype='object'),
        T_SHIP_TYPE_COL: pd.Series(dtype='object'),
        T_SHIP_NAME_COL: pd.Series(dtype='object'),
        T_SHIP_CALLSIGN_COL: pd.Series(dtype='object'),
        T_A_COL: pd.Series(dtype='float64'),
        T_B_COL: pd.Series(dtype='float64'),
        T_C_COL: pd.Series(dtype='float64'),
        T_D_COL: pd.Series(dtype='float64')
    })