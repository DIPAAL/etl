"""Module defining cell benchmark configurations."""
from benchmarks.dataclasses.geolimits import GeoLimits
from benchmarks.enumerations.cell_benchmark_configuration_type import CellBenchmarkConfigurationType
from typing import List, Dict
from etl.helper_functions import get_staging_cell_sizes


class CellBenchmarkConfiguration:
    """Class defining how a cell benchmark should be run."""

    def __init__(self, start_date_id: int, end_date_id: int, geolimits: GeoLimits, ship_types: List[str],
                 configuration_type: CellBenchmarkConfigurationType, spatial_resolution: int | None = None) -> None:
        """
        Initialize a cell benchmark configuration.

        Arguments:
            start_date_id: the data warehouse date smart ID for the start date used in benchmark queries
            end_date_id: the data warehouse date smart ID for the end date used in benchmark queries
            geolimits: the spatial limits for the configuration
            ship_types: a list of ship types used to filter the benchmark queries
            configuration_type: the type for the configuration
            spatial_resolution: the spatial resolution for the configuration (default: None)
        """
        self.start_date_id = start_date_id
        self.end_date_id = end_date_id
        self.spatial_resolution = spatial_resolution
        self.geolimits = geolimits
        self.ship_types = ship_types
        self.type = configuration_type
        self._validate_spatial_resolution(spatial_resolution)

    def _validate_spatial_resolution(self, resolution: int) -> None:
        """
        Validate the spatial resolution of the configuration.

        Arguments:
            resolution: the given spatial resolution
        """
        if CellBenchmarkConfigurationType.CELL == self.type \
                and resolution not in get_staging_cell_sizes():
            raise ValueError(f'Invalid spatial resolution <{resolution}> provided for CellBenchmarkConfiguration. '
                             f'Only <{get_staging_cell_sizes()}> are supported')

    def get_parameters(self) -> Dict[str, any]:
        """Return query parameters based on configuration."""
        return {
            'START_ID': self.start_date_id,
            'END_ID': self.end_date_id,
            'XMIN': self.geolimits.xmin,
            'YMIN': self.geolimits.ymin,
            'XMAX': self.geolimits.xmax,
            'YMAX': self.geolimits.ymax,
            'SHIP_TYPES': self.ship_types
        }

    def format_query(self, query: str) -> str:
        """
        Format the query based on the configuration.

        Arguments:
            query: the query to format
        """
        if CellBenchmarkConfigurationType.CELL == self.type:
            return query.format(CELL_SIZE=self.spatial_resolution)
        return query
