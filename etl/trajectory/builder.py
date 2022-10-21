import geopandas as gpd
import pandas as pd
from mobilitydb import TGeomPointSeq, TFloatInstSet, TFloatInst
from typing import Callable, Optional
from etl.cleaning.clean_data import COORDINATE_REFERENCE_SYSTEM, CVS_TIMESTAMP_FORMAT
import math
from datetime import datetime

SPEED_THRESHOLD_KNOTS=100

MOBILITYDB_TIMESTAMP_FORMAT='%Y-%m-%d %H:%M:%S' #2020-01-01 00:00:00+01
COORDINATE_REFERENCE_SYSTEM_METERS='epsg:3034'
KNOTS_PER_METER_SECONDS=1.943844 # = 1 m/s
COMPUTED_VS_SOG_KNOTS_THRESHOLD=2
STOPPED_KNOTS_THRESHOLD=0.5
STOPPED_TIME_SECONDS_THRESHOLD=5*60 # 5 minutes
SPLIT_GAP_SECONDS_THRESHOLD=5*60 # 5 minutes

class _PointCompare:
    def __init__(self, row: gpd.GeoDataFrame) -> None:
        self.long = row['Longitude']
        self.lat = row['Latitude']
        self.time = row['Timestamp']
        self.speed_over_ground = row['SOG']
        
    def get_long(self): return self.long
    def get_lat(self): return self.lat
    def get_time(self): return self.time
    def get_sog(self): return self.speed_over_ground


def build_from_geopandas(clean_sorted_ais: gpd.GeoDataFrame) -> pd.DataFrame:
    grouped_data = clean_sorted_ais.groupby(by='MMSI')
    pd.set_option('display.max_columns', 100)

    result_frames = []
    for mmsi, ship_data in grouped_data:
        ship_data.reset_index(inplace=True)
        result_frames.append(_create_trajectory(mmsi=mmsi, data=ship_data))

    return pd.concat(result_frames)

def _create_trajectory(mmsi: int, data: gpd.GeoDataFrame) -> pd.DataFrame:
    dataframe = _remove_outliers(dataframe=data)

    return _construct_moving_trajectory(dataframe, 0)
        
def _construct_moving_trajectory(trajectory_dataframe: gpd.GeoDataFrame, from_idx: int) -> pd.DataFrame:
    idx_cannot_handle = None
    for idx in range(from_idx, len(trajectory_dataframe.index)):
        row = trajectory_dataframe.iloc[idx]
        
        if row['SOG'] < STOPPED_KNOTS_THRESHOLD:
            if idx_cannot_handle is not None:
                current_date = row['Timestamp']
                if (current_date - trajectory_dataframe.iloc[idx_cannot_handle]['Timestamp']).seconds >= STOPPED_TIME_SECONDS_THRESHOLD:
                    trajectory = _finalize_trajectory(trajectory_dataframe, from_idx, idx_cannot_handle, infer_stopped=False)
                    trajectories = _construct_stopped_trajectory(trajectory_dataframe, idx_cannot_handle)
                    return pd.concat(trajectory, trajectories)
        else:
            idx_cannot_handle = None
    
    return _finalize_trajectory(trajectory_dataframe, from_idx, len(trajectory_dataframe.index), infer_stopped=False)

def _finalize_trajectory(trajectory_dataframe: gpd.GeoDataFrame, from_idx: int, to_idx: int, infer_stopped: bool) -> pd.DataFrame:
    to_idx -= 1 # to_idx is exclusive
    dataframe = _create_trajectory_db_df()
    working_dataframe = trajectory_dataframe.truncate(before=from_idx, after=to_idx)

    trajectory = _convert_dataframe_to_trajectory(working_dataframe)
    start_datetime = trajectory_dataframe.iloc[from_idx]['Timestamp']
    end_datetime = trajectory_dataframe.iloc[to_idx]['Timestamp']

    # Groupby: eta, nav_status, draught, destination
    sorted_series_by_frequency = _find_most_recuring(working_dataframe)
    eta = sorted_series_by_frequency['ETA'][0]
    nav_status = sorted_series_by_frequency['Navigational status'][0]
    draught = sorted_series_by_frequency['Draught'][0]
    destination = sorted_series_by_frequency['Destination'][0]

    # Split eta, start_datetime, and end_datetime and create their smart keys
    eta_date_id = _extract_date_smart_id(eta)
    eta_time_id = _extract_time_smart_id(eta)
    start_date_id = _extract_date_smart_id(start_datetime)
    start_time_id = _extract_time_smart_id(start_datetime)
    end_date_id = _extract_date_smart_id(end_datetime)
    end_time_id = _extract_time_smart_id(end_datetime)

    duration = end_datetime - start_datetime
    rot = _tfloat_from_dataframe(working_dataframe, 'ROT')
    heading = _tfloat_from_dataframe(working_dataframe, 'Heading')

    return pd.concat([dataframe, pd.Series(data={
                                           'start_date_id': start_date_id, 'start_time_id': start_time_id,
                                           'end_date_id':end_date_id, 'end_time_id':end_time_id,
                                           'eta_date_id':eta_date_id, 'eta_time_id':eta_time_id,
                                           'nav_status':nav_status, 'duration':duration,
                                           'trajectory':trajectory, 'infer_stopped':infer_stopped,
                                           'destination':destination, 'rot':rot,
                                           'heading':heading, 'draught':draught
                                        }).to_frame().T])

def _extract_date_smart_id(datetime: datetime) -> int:
    return (datetime.year * 10000) + (datetime.month * 100) + (datetime.day)

def _extract_time_smart_id(datetime: datetime) -> int:
    return (datetime.hour * 10000) + (datetime.minute * 100) + (datetime.second)

def _tfloat_from_dataframe(dataframe: gpd.GeoDataFrame, float_column:str) -> TFloatInstSet:
    tfloat_lst = []
    for _, row in dataframe.iterrows():
        mobilitydb_timestamp = row['Timestamp'].strftime(MOBILITYDB_TIMESTAMP_FORMAT) + '+01'
        float_val = row[float_column].astype(str)
        tfloat_lst.append(TFloatInst(str(float_val + '@' + mobilitydb_timestamp)))
    
    TFloatInstSet(*tfloat_lst)

def _find_most_recuring(trajectory_dataframe: gpd.GeoDataFrame) -> pd.Series:
    return trajectory_dataframe.value_counts(subset=['ETA', 'Navigational status', 'Draught', 'Destination'], sort=True, dropna=False).index.to_frame()

def _construct_stopped_trajectory(trajectory_dataframe: gpd.GeoDataFrame, from_idx: int) -> pd.DataFrame:
    for idx in range(from_idx, len(trajectory_dataframe.index)):
        row = trajectory_dataframe.iloc[idx]

        if row['SOG'] >= STOPPED_KNOTS_THRESHOLD:
            stopped_trajectory = _finalize_trajectory(trajectory_dataframe, from_idx, idx, infer_stopped=True)
            trajectories = _construct_moving_trajectory(trajectory_dataframe, idx)
            return pd.concat(stopped_trajectory, trajectories)

    stopped_trajectory = _finalize_trajectory(trajectory_dataframe, from_idx, len(trajectory_dataframe.index), infer_stopped=True)
    return stopped_trajectory 

def _convert_dataframe_to_trajectory(trajectory_dataframe: pd.DataFrame) -> TGeomPointSeq:
    mobilitydb_dataframe = pd.DataFrame(columns=['tgeompoint'])
    mobilitydb_dataframe['Timestamp'] = trajectory_dataframe['Timestamp'].apply(func=lambda t: t.strftime(MOBILITYDB_TIMESTAMP_FORMAT))
    mobilitydb_dataframe['tgeompoint'] = trajectory_dataframe['geometry'].astype(str) + '@' + mobilitydb_dataframe['Timestamp']

    mobility_str = f"[{','.join(mobilitydb_dataframe['tgeompoint'])}]"

    return TGeomPointSeq(mobility_str)

    
def _rebuild_to_geodataframe(pandas_dataframe: pd.DataFrame) -> gpd.GeoDataFrame:
    pandas_dataframe.drop(labels='geometry', axis='columns', inplace=True)
    return gpd.GeoDataFrame(data=pandas_dataframe, geometry=gpd.points_from_xy(x=pandas_dataframe['Longitude'], y=pandas_dataframe['Latitude'], crs=COORDINATE_REFERENCE_SYSTEM))


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
    #print(f'Distance in meters={distance}')
    #print(f'Calculated speed={speed}')

    # The other group uses SOG if the absolute difference is above a threshold
    if abs((cur_point.get_sog() - speed) > COMPUTED_VS_SOG_KNOTS_THRESHOLD):
        speed = cur_point.get_sog()

    #print(f'Used speed={speed}')

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
        'draught': pd.Series(dtype='float64')
    })

# Outline
#   Group on the MMSI to get all the data points for a single ship (Done)
#   Construct a trajectory by looping through the data (Done)
#   Remove outliers if to far away from prevous point (Distance Function) (Done)
#   Detect whether a part of a trajectory is stopped or is sailing


# Structure in Database
#   ship_id integer NOT NULL,
#   start_date_id integer NOT NULL,
#   start_time_id integer NOT NULL,
#   end_date_id integer NOT NULL,
#   end_time_id integer NOT NULL,
#   eta_date_id integer NOT NULL,
#   eta_time_id integer NOT NULL,
#   nav_status_id smallint NOT NULL,
#   
#   duration interval NOT NULL,
#   trajectory tgeompoint NOT NULL,
#   infer_stopped boolean NOT NULL,
#   destination text NOT NULL,
#   rot tfloat NOT NULL,
#   heading tfloat NOT NULL,
#   draught float NOT NULL,
