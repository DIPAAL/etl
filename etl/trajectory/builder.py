import geopandas as gpd
import pandas as pd
import math
from datetime import datetime
from mobilitydb import TGeomPointSeq, TFloatInstSet, TFloatInst
from typing import Callable, Optional, List
from etl.constants import COORDINATE_REFERENCE_SYSTEM, LONGITUDE_COL, LATITUDE_COL, TIMESTAMP_COL, SOG_COL, MMSI_COL, ETA_COL, DESTINATION_COL, NAVIGATIONAL_STATUS_COL, DRAUGHT_COL, ROT_COL, HEADING_COL, IMO_COL, POSITION_FIXING_DEVICE_COL, SHIP_TYPE_COL, NAME_COL, CALLSIGN_COL, A_COL, B_COL, C_COL, D_COL, MBDB_TRAJECTORY_COL, GEO_PANDAS_GEOMETRY_COL

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

class _PointCompare:
    def __init__(self, row: gpd.GeoDataFrame) -> None:
        self.long = row[LONGITUDE_COL]
        self.lat = row[LATITUDE_COL]
        self.time = row[TIMESTAMP_COL]
        self.speed_over_ground = row[SOG_COL]
        
    def get_long(self): return self.long
    def get_lat(self): return self.lat
    def get_time(self): return self.time
    def get_sog(self): return self.speed_over_ground


def build_from_geopandas(clean_sorted_ais: gpd.GeoDataFrame) -> pd.DataFrame:
    grouped_data = clean_sorted_ais.groupby(by=MMSI_COL)

    result_frames = []
    for mmsi, ship_data in grouped_data:
        ship_data.reset_index(inplace=True)
        result_frames.append(_create_trajectory(mmsi=mmsi, data=ship_data))

    return pd.concat(result_frames)

def _create_trajectory(mmsi: int, data: gpd.GeoDataFrame) -> pd.DataFrame:
    dataframe = _remove_outliers(dataframe=data)

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
                    return pd.concat(trajectory, trajectories)
        else:
            idx_cannot_handle = None
    
    return _finalize_trajectory(mmsi, trajectory_dataframe, from_idx, len(trajectory_dataframe.index), infer_stopped=False)

def _finalize_trajectory(mmsi: int, trajectory_dataframe: gpd.GeoDataFrame, from_idx: int, to_idx: int, infer_stopped: bool) -> pd.DataFrame:
    to_idx -= 1 # to_idx is exclusive
    dataframe = _create_trajectory_db_df()
    working_dataframe = trajectory_dataframe.truncate(before=from_idx, after=to_idx)

    trajectory = _convert_dataframe_to_trajectory(working_dataframe)
    start_datetime = trajectory_dataframe.iloc[from_idx][TIMESTAMP_COL]
    end_datetime = trajectory_dataframe.iloc[to_idx][TIMESTAMP_COL]

    # Groupby: eta, nav_status, draught, destination
    column_subset = [ETA_COL, NAVIGATIONAL_STATUS_COL, DRAUGHT_COL, DESTINATION_COL]
    sorted_series_by_frequency = _find_most_recuring(working_dataframe, column_subset=column_subset, drop_na = False)
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
    most_recuring = _find_most_recuring(trajectory_dataframe=trajectory_dataframe, column_subset=[IMO_COL], drop_na=True)
    imo = most_recuring[IMO_COL][0] if most_recuring.size != 0 else UNKNOWN_INT_VALUE

    most_recuring = _find_most_recuring(trajectory_dataframe=trajectory_dataframe, column_subset=[POSITION_FIXING_DEVICE_COL], drop_na=True)
    mobile_type = most_recuring[POSITION_FIXING_DEVICE_COL][0] if most_recuring.size != 0 else UNKNOWN_STRING_VALUE

    most_recuring = _find_most_recuring(trajectory_dataframe=trajectory_dataframe, column_subset=[SHIP_TYPE_COL], drop_na=True)
    ship_type = most_recuring[SHIP_TYPE_COL][0] if most_recuring.size != 0 else UNKNOWN_STRING_VALUE

    most_recuring = _find_most_recuring(trajectory_dataframe=trajectory_dataframe, column_subset=[NAME_COL], drop_na=True)
    ship_name = most_recuring[NAME_COL][0] if most_recuring.size != 0 else UNKNOWN_STRING_VALUE

    most_recuring = _find_most_recuring(trajectory_dataframe=trajectory_dataframe, column_subset=[CALLSIGN_COL], drop_na=True)
    ship_callsign = most_recuring[CALLSIGN_COL][0] if most_recuring.size != 0 else UNKNOWN_STRING_VALUE

    most_recuring = _find_most_recuring(trajectory_dataframe=trajectory_dataframe, column_subset=[A_COL], drop_na=True)
    a = most_recuring[A_COL].iloc[0] if most_recuring.size != 0 else UNKNOWN_FLOAT_VALUE

    most_recuring = _find_most_recuring(trajectory_dataframe=trajectory_dataframe, column_subset=[B_COL], drop_na=True)
    b = most_recuring[B_COL].iloc[0] if most_recuring.size != 0 else UNKNOWN_FLOAT_VALUE

    most_recuring = _find_most_recuring(trajectory_dataframe=trajectory_dataframe, column_subset=[C_COL], drop_na=True)
    c = most_recuring[C_COL].iloc[0] if most_recuring.size != 0 else UNKNOWN_FLOAT_VALUE

    most_recuring = _find_most_recuring(trajectory_dataframe=trajectory_dataframe, column_subset=[D_COL], drop_na=True)
    d = most_recuring[D_COL].iloc[0] if most_recuring.size != 0 else UNKNOWN_FLOAT_VALUE


    return pd.concat([dataframe, pd.Series(data={
                                           'start_date_id': start_date_id, 'start_time_id': start_time_id,
                                           'end_date_id':end_date_id, 'end_time_id':end_time_id,
                                           'eta_date_id':eta_date_id, 'eta_time_id':eta_time_id,
                                           'nav_status':nav_status, 'duration':duration,
                                           'trajectory':trajectory, 'infer_stopped':infer_stopped,
                                           'destination':destination, 'rot':rot,
                                           'heading':heading, 'draught':draught,
                                           'mmsi': mmsi, 'imo': imo,
                                           'mobile_type': mobile_type, 'ship_type': ship_type,
                                           'ship_name': ship_name, 'ship_callsign': ship_callsign,
                                           'a': a, 'b': b, 'c': c, 'd': d
                                        }).to_frame().T])

def _extract_date_smart_id(datetime: datetime) -> int:
    return (datetime.year * 10000) + (datetime.month * 100) + (datetime.day)

def _extract_time_smart_id(datetime: datetime) -> int:
    return (datetime.hour * 10000) + (datetime.minute * 100) + (datetime.second)

def _tfloat_from_dataframe(dataframe: gpd.GeoDataFrame, float_column:str) -> TFloatInstSet:
    tfloat_lst = []
    for _, row in dataframe.iterrows():
        mobilitydb_timestamp = row[TIMESTAMP_COL].strftime(MOBILITYDB_TIMESTAMP_FORMAT) + '+01'
        float_val = row[float_column].astype(str)
        tfloat_lst.append(TFloatInst(str(float_val + '@' + mobilitydb_timestamp)))
    
    TFloatInstSet(*tfloat_lst)

def _find_most_recuring(trajectory_dataframe: gpd.GeoDataFrame, column_subset: List[str], drop_na: bool) -> pd.Series:
    return trajectory_dataframe.value_counts(subset=column_subset, sort=True, dropna=drop_na).index.to_frame()

def _construct_stopped_trajectory(mmsi: int, trajectory_dataframe: gpd.GeoDataFrame, from_idx: int) -> pd.DataFrame:
    for idx in range(from_idx, len(trajectory_dataframe.index)):
        row = trajectory_dataframe.iloc[idx]

        if row[SOG_COL] >= STOPPED_KNOTS_THRESHOLD:
            stopped_trajectory = _finalize_trajectory(mmsi, trajectory_dataframe, from_idx, idx, infer_stopped=True)
            trajectories = _construct_moving_trajectory(mmsi, trajectory_dataframe, idx)
            return pd.concat(stopped_trajectory, trajectories)

    stopped_trajectory = _finalize_trajectory(mmsi, trajectory_dataframe, from_idx, len(trajectory_dataframe.index), infer_stopped=True)
    return stopped_trajectory 

def _convert_dataframe_to_trajectory(trajectory_dataframe: pd.DataFrame) -> TGeomPointSeq:
    mobilitydb_dataframe = pd.DataFrame(columns=[MBDB_TRAJECTORY_COL])
    mobilitydb_dataframe[TIMESTAMP_COL] = trajectory_dataframe[TIMESTAMP_COL].apply(func=lambda t: t.strftime(MOBILITYDB_TIMESTAMP_FORMAT))
    mobilitydb_dataframe[MBDB_TRAJECTORY_COL] = trajectory_dataframe[GEO_PANDAS_GEOMETRY_COL].astype(str) + '@' + mobilitydb_dataframe[TIMESTAMP_COL]

    mobility_str = f"[{','.join(mobilitydb_dataframe['tgeompoint'])}]"

    return TGeomPointSeq(mobility_str)

    
def _rebuild_to_geodataframe(pandas_dataframe: pd.DataFrame) -> gpd.GeoDataFrame:
    pandas_dataframe.drop(labels=GEO_PANDAS_GEOMETRY_COL, axis='columns', inplace=True)
    return gpd.GeoDataFrame(data=pandas_dataframe, geometry=gpd.points_from_xy(x=pandas_dataframe[LONGITUDE_COL], y=pandas_dataframe[LATITUDE_COL], crs=COORDINATE_REFERENCE_SYSTEM))


def _remove_outliers(dataframe: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    prev_row: Optional[pd.Series] = None
    result_dataframe = pd.DataFrame(columns=dataframe.columns)

    for idx in range(0, len(dataframe.index)):
        row = dataframe.iloc[idx]

        if prev_row is None: # this is the first point
            prev_row = row
            result_dataframe = pd.concat([result_dataframe, row.to_frame().T])
            continue

        
        if not _check_outlier(cur_point=_PointCompare(row), prev_point=_PointCompare(prev_row), speed_threshold=SPEED_THRESHOLD_KNOTS, dist_func=_euclidian_dist):
            prev_row = row
            result_dataframe = pd.concat([result_dataframe, row.to_frame().T])

    return _rebuild_to_geodataframe(result_dataframe)

def _check_outlier(cur_point: _PointCompare, prev_point: _PointCompare, speed_threshold: float, dist_func: Callable[[float, float, float, float], float]) -> bool:
    """
    Checks whether the distance between two points, given the provided distance function and threshold, is an outlier
        cur_point: Point as a _PointCompare object
        prev_point: Point as a _PointCompare object
        dist_threshold: Threshold distance which determines whether distance between the points indicates an outlier
        dist_function: A distance function that takes in 4 parameters (cur_long, cur_lat, prev_long, prev_lat) and returns the distance between the points

        Returns: A bool indicating that an outlier is detected
    """
    cur_point_converted_long_lat = gpd.points_from_xy(x=[cur_point.get_long()], y=[cur_point.get_lat()], crs=COORDINATE_REFERENCE_SYSTEM_METERS)
    prev_point_converted_long_lat = gpd.points_from_xy(x=[prev_point.get_long()], y=[prev_point.get_lat()], crs=COORDINATE_REFERENCE_SYSTEM_METERS)
    
    time_delta = cur_point.get_time() - prev_point.get_time()
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
        'start_date_id': pd.Series(dtype='int64'),
        'start_time_id': pd.Series(dtype='int64'),
        'end_date_id': pd.Series(dtype='int64'),
        'end_time_id': pd.Series(dtype='int64'),
        'eta_date_id': pd.Series(dtype='int64'),
        'eta_time_id': pd.Series(dtype='int64'),
        'nav_status': pd.Series(dtype='object'),
        # Measures
        'duration': pd.Series(dtype='timedelta64[ns]'),
        'trajectory': pd.Series(dtype='object'),
        'infer_stopped': pd.Series(dtype='bool'),
        'destination': pd.Series(dtype='object'),
        'rot': pd.Series(dtype='object'),
        'heading': pd.Series(dtype='object'),
        'draught': pd.Series(dtype='float64'),
        # Ship
        'imo': pd.Series(dtype='int64'),
        'mmsi': pd.Series(dtype='int64'),
        'mobile_type': pd.Series(dtype='object'),
        'ship_type': pd.Series(dtype='object'),
        'ship_name': pd.Series(dtype='object'),
        'ship_callsign': pd.Series(dtype='object'),
        'a': pd.Series(dtype='float64'),
        'b': pd.Series(dtype='float64'),
        'c': pd.Series(dtype='float64'),
        'd': pd.Series(dtype='float64')
    })