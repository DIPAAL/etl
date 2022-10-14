from typing import Dict, Tuple

class Trajectory:
    def __init__(self, start_timestamp:str, mmsi:int):
        self.start_timestamp: str = start_timestamp
        self.end_timestamp:str = 'inf'
        self.points: Dict[str, (float, float)] = {}
        self.ship_mmsi:int = mmsi

    def add_point(self, long:float, lat:float, timestamp:str):
        if timestamp in self.points:
            print('Tried to add timestamp that already exists in trajectory, dropped it')
            return

        self.points[timestamp] = (long, lat)
    
    def get_ship_mmsi(self):
        return self.ship_mmsi
    
    def get_trajectory_range(self) -> Tuple[str, str]:
        return (self.start_timestamp, self.end_timestamp)

    def get_points(self) -> Dict[str, (float, float)]:
        return self.points