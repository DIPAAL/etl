import geopandas as gpd
import pandas as pd
from mobilitydb import TGeomPointSeq
from typing import Callable, Optional, List
from etl.cleaning.clean_data import COORDINATE_REFERENCE_SYSTEM
import math
from datetime import datetime

SPEED_THRESHOLD_KNOTS=100
TIMESTAMP_FORMAT='%d/%m/%Y %H:%M:%S' #07/09/2021 00:00:00
MOBILITYDB_TIMESTAMP_FORMAT='%Y-%m-%d %H:%M:%S' #2020-01-01 00:00:00+01
COORDINATE_REFERENCE_SYSTEM_METERS='epsg:25832'
KNOTS_PER_METER_SECONDS=1.943844 # = 1 m/s
COMPUTED_VS_SOG_KNOTS_THRESHOLD=2
STOPPED_KNOTS_THRESHOLD=0.5
STOPPED_TIME_SECONDS_THRESHOLD=5*60 # 5 minutes
SPLIT_GAP_SECONDS_THRESHOLD=5*60 # 5 minutes

class _PointCompare:
    def __init__(self, long:float, lat:float, timestamp:str, sog:float) -> None:
        self.long = long
        self.lat = lat
        self.time = datetime.strptime(timestamp, TIMESTAMP_FORMAT)
        self.speed_over_ground = sog
    
    def __init__(self, row: gpd.GeoDataFrame) -> None:
        self.long = row['Longitude']
        self.lat = row['Latitude']
        self.time = datetime.strptime(row['# Timestamp'], TIMESTAMP_FORMAT)
        self.speed_over_ground = row['SOG']
        
    def get_long(self): return self.long
    def get_lat(self): return self.lat
    def get_time(self): return self.time
    def get_sog(self): return self.speed_over_ground


def build_from_geopandas(clean_sorted_ais: gpd.GeoDataFrame):
    grouped_data = clean_sorted_ais.groupby(by='MMSI')
    pd.set_option('display.max_columns', 50)
    
    print(f'grouped_data: {grouped_data}')
    for mmsi, _ in grouped_data:
        print(f'mmsi={mmsi}')
        ship_data = grouped_data.get_group(name=mmsi)
        print(f'ship_data length={len(ship_data.index)}')
        _create_trajectory(mmsi=mmsi, data=ship_data)
        # TEMPORARY
        #return


def _create_trajectory(mmsi: int, data: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    dataframe = _remove_outliers(dataframe=data)
    _split_trajectories(dataframe)

    return dataframe

def _split_trajectories(dataframe: gpd.GeoDataFrame):
    # Run through the trajectories
    # Check for split conditions
    # When split create trajectory (MobilityDB)

    trajectory_points = pd.DataFrame(columns=dataframe.columns)
    trajectories: List[TGeomPointSeq] = []
    prev_point: Optional(pd.Series) = None

    idx_last_speed_below_stopped_threshold = None
    datetime_last_speed_below_stopped_threshold = None

    for idx in range(0,len(dataframe.index)):
        row = dataframe.iloc[idx]

        if prev_point is None:
            trajectory_points = pd.concat([trajectory_points, row.to_frame().T])
            prev_point = row
            if row['SOG'] < STOPPED_KNOTS_THRESHOLD:
                idx_last_speed_below_stopped_threshold = idx
                datetime_last_speed_below_stopped_threshold = datetime.strptime(row['# Timestamp'], TIMESTAMP_FORMAT)
            continue

        # Z-test
        if (_PointCompare(prev_point).get_time() - _PointCompare(row).get_time()).seconds > STOPPED_TIME_SECONDS_THRESHOLD:
            trajectories.append(_convert_dataframe_to_trajectory(trajectory_points))
            trajectory_points.truncate()
        
        


        

def _convert_dataframe_to_trajectory(trajectory_dataframe: pd.DataFrame) -> TGeomPointSeq:
    mobilitydb_dataframe = pd.DataFrame(columns=['tgeompoint'])
    mobilitydb_dataframe['Timestamp'] = trajectory_dataframe['# Timestamp']
    mobilitydb_dataframe['Timestamp'] = mobilitydb_dataframe['Timestamp'].apply(func=lambda t: datetime.strptime(t, TIMESTAMP_FORMAT).strftime(MOBILITYDB_TIMESTAMP_FORMAT))
    mobilitydb_dataframe['tgeompoint'] = trajectory_dataframe['geometry'].astype(str) + '@' + mobilitydb_dataframe['Timestamp']

    mobility_str = '['
    first = True
    for idx in range(0, len(mobilitydb_dataframe.index)):
        row = mobilitydb_dataframe.iloc[idx]
        mobility_str += row['tgeompoint']
        if not first:
            mobility_str += ','
        first = False
    
    mobility_str += ']'

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
    
    distance = dist_func(cur_point_converted_long_lat.x, cur_point_converted_long_lat.y, prev_point_converted_long_lat.x, prev_point_converted_long_lat.y)
    time_delta = cur_point.get_time() - prev_point.get_time()

    # Previous and current point is in the same timestamp, detect it as an outlier
    if time_delta.seconds == 0:
        return True

    computed_speed = distance/time_delta.seconds # m/s
    speed = computed_speed * KNOTS_PER_METER_SECONDS
    print(f'Distance in meters={distance}')
    print(f'Calculated speed={speed}')

    # The other group uses SOG if the absolute difference is above a threshold
    if abs((cur_point.get_sog() - speed) > COMPUTED_VS_SOG_KNOTS_THRESHOLD):
        speed = cur_point.get_sog()

    print(f'Used speed={speed}')

    if speed > speed_threshold:
        return True
    return False

def _euclidian_dist(a_long:float, a_lat:float, b_long:float , b_lat:float) -> float:
    return math.sqrt(
        (math.pow((b_long - a_long), 2) + math.pow((b_lat - a_lat), 2))
    )



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