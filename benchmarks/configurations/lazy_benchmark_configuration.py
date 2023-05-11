"""Module containing the configuration class for lazy 50m benchmarks."""
from datetime import datetime
from etl.helper_functions import extract_smart_date_id_from_date, extract_smart_time_id_from_date
from typing import Dict, Any


class LazyBenchmarkConfiguration:
    """Class defining how a lazy 50m benchmark should be run."""

    def __init__(self, xmin: float, ymin: float, xmax: float, ymax: float,
                 start_timestamp: datetime, end_timestamp: datetime):
        """
        Initialize lazy benchmark configuration.

        Arguments:
            xmin: the lower left x-coordinate in project ESPG:3034 of the area benchmarked
            ymin: the lower left y-coordinate in project ESPG:3034 of the area benchmarked
            xmax: the upper right x-coordinate in project ESPG:3034 of the area benchmarked
            ymax: the upper right y-coordinate in project ESPG:3034 of the area benchmarked
            start_timestamp: timestamp that specifies the start of the period to benchmark
            end_timestamp: timestamp that specifies the end of the period to benchmark
        """
        self.xmin = xmin
        self.ymin = ymin
        self.xmax = xmax
        self.ymax = ymax
        self.start_date_id = extract_smart_date_id_from_date(start_timestamp)
        self.start_time_id = extract_smart_time_id_from_date(start_timestamp)
        self.end_date_id = extract_smart_date_id_from_date(end_timestamp)
        self.end_time_id = extract_smart_time_id_from_date(end_timestamp)

    def get_parameters(self) -> Dict[str, Any]:
        """Return the parameters for the configuration."""
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
