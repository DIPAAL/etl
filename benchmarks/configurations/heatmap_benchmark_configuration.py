"""Module containing definition for Heatmap benchmark configurations."""
from typing import Dict, Any, List

from benchmarks.dataclasses.geolimits import GeoLimits
from etl.helper_functions import get_staging_cell_sizes


class HeatmapBenchmarkConfiguration:
    """Contains a heatmap benchmark configuration."""

    def __init__(self, start_date_id: int, end_date_id: int, spatial_resolution: int, geolimits: GeoLimits,
                 heatmap_type: str, ship_types_a: List[str], ship_types_b: List[str], mobile_types: List[str]) -> None:
        """
        Initialize Heatmap benchmark configuration.

        Arguments:
            start_date_id: the data warehouse date smart ID for the start date used in benchmark queries
            end_date_id: the data warehouse date smart ID for the end date used in benchmark queries
            spatial_resolution: the spatial resolution for the configuration
            geolimits: the spatial limits for the configuration
            heatmap_type: the type of heatmap to create for the benchmark
            ship_types_a: list of ship types used to filter the benchmark queries for the substractee raster
            ship_types_b: list of ship types used to filter the benchmark queries for the substracting raster
            mobile_types: list of mobile types used to filter the benchmark queries
        """
        self.start_date_id = start_date_id
        self.end_date_id = end_date_id
        self.resolution = spatial_resolution
        self.geolimits = geolimits
        self.heatmap_type = heatmap_type
        self.ship_types_a = ship_types_a
        self.ship_types_b = ship_types_b
        self.mobile_types = mobile_types
        self._validate_spatial_resolution()

    def _validate_spatial_resolution(self) -> None:
        """Validate the spatial resolution of the configuration."""
        if self.resolution not in get_staging_cell_sizes():
            raise ValueError(f'Invalid spatial resolution <{self.resolution}> provided for CellBenchmarkConfiguration. '
                             f'Only <{get_staging_cell_sizes()}> are supported')

    def get_parameters(self) -> Dict[str, Any]:
        """Return query parameters based on configuration."""
        return {
            'START_ID': self.start_date_id,
            'END_ID': self.end_date_id,
            'SPATIAL_RESOLUTION': self.resolution,
            'XMIN': self.geolimits.xmin,
            'YMIN': self.geolimits.ymin,
            'XMAX': self.geolimits.xmax,
            'YMAX': self.geolimits.ymax,
            'HEATMAP_TYPE': self.heatmap_type,
            'SHIP_TYPES_A': self.ship_types_a,
            'SHIP_TYPES_B': self.ship_types_b,
            'MOBILE_TYPES': self.mobile_types
        }
