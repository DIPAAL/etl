""""""
from typing import Dict, Any, List
from etl.helper_functions import get_staging_cell_sizes


class HeatmapBenchmarkConfiguration:
    """"""

    def __init__(self, start_date_id: int, end_date_id: int, spatial_resolution: int, area: int, heatmap_type: str, ship_types: List[str], mobile_types: List[str]) -> None:
        self.start_date_id = start_date_id
        self.end_date_id = end_date_id
        self.resolution = spatial_resolution
        self.area = area
        self.heatmap_type = heatmap_type
        self.ship_types = ship_types
        self.mobile_types = mobile_types
        self.__validate_spatial_resolution()

    def __validate_spatial_resolution(self) -> None:
        """"""
        if self.resolution not in get_staging_cell_sizes():
            raise ValueError(f'Invalid spatial resolution <{self.resolution}> provided for CellBenchmarkConfiguration. '
                             'Only <{self.VALID_RESOLUTIONS}> are supported')

    def get_parameters(self) -> Dict[str, Any]:
        """"""
        return {
            'START_ID': self.start_date_id,
            'END_ID': self.end_date_id,
            'SPATIAL_RESOLUTION': self.resolution,
            'AREA_ID': self.area,
            'HEATMAP_TYPE': self.heatmap_type,
            'SHIP_TYPES': self.ship_types,
            'MOBILE_TYPES': self.mobile_types
        }
