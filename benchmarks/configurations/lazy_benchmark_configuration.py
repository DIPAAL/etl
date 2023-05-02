from datetime import datetime
from etl.helper_functions import extract_smart_date_id_from_date, extract_smart_time_id_from_date
from typing import Dict, Any

class LazyBenchmarkConfiguration:
    def __init__(self, xmin: int, ymin: int, xmax: int, ymax: int, start_timestamp: datetime, end_timestamp: datetime):
        self.xmin = xmin
        self.ymin = ymin
        self.xmax = xmax
        self.ymax = ymax
        self.start_date_id = extract_smart_date_id_from_date(start_timestamp)
        self.start_time_id = extract_smart_time_id_from_date(start_timestamp)
        self.end_date_id = extract_smart_date_id_from_date(end_timestamp)
        self.end_time_id = extract_smart_time_id_from_date(end_timestamp)

    def get_parameters(self) -> Dict[str, Any]:
        return {
            'xmin': self.xmin,
            'ymin': self.ymin,
            'xmax': self.xmax,
            'ymax': self.ymax,
            'start_date': self.start_date_id,
            'start_time': self.start_time_id,
            'end_date': self.end_date_id,
            'end_time': self.end_time_id
        }