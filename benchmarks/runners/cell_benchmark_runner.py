"""Module containing the cell benchmark runner."""
from enum import Enum
from benchmarks.benchmark_runner import AbstractRuntimeBenchmarkRunner, RuntimeBenchmarkResult
from typing import Dict, List, Callable
from etl.helper_functions import measure_time
from sqlalchemy import text


class CellBenchmarkConfigurationType(Enum):
    """Enumeration defining possible cell benchmark configuration types."""

    CELL = 'cell'
    TRAJECTORY = 'traj'

    def __str__(self) -> str:
        """Return enumeration value as string representation."""
        return self.value


class CellBenchmarkConfiguration:
    """Class defining how a cell benchmark should be run."""

    VALID_RESOLUTIONS = [5000, 1000, 200, 50]

    def __init__(self, start_date_id: int, end_date_id: int, enc_cell_id: int, ship_types: List[str],
                 configuration_type: CellBenchmarkConfigurationType, spatial_resolution: int | None = None) -> None:
        """
        Initialize a cell benchmark configuration.

        Arguments:
            start_date_id: the data warehouse date smart ID for the start date used in benchmar queries
            end_date_id: the data warehouse date smart ID for the end date used in benchmark queries
            enc_cell_id: the ID of the enc cell determining the spatial area of the benchmark queries
            ship_types: a list of ship types used to filter the benchmark queries
            configuration_type: the type for the configuration
            spatial_resolution: the spatial resolution for the configuration (default: None)
        """
        self.start_date_id = start_date_id
        self.end_date_id = end_date_id
        self.spatial_resolution = spatial_resolution
        self.enc_cell_id = enc_cell_id
        self.ship_types = ship_types
        self.type = configuration_type
        self.__validate_spatial_resolution(spatial_resolution)

    def __validate_spatial_resolution(self, resolution: int) -> None:
        """
        Validate the spatial resolution of the configuration.

        Arguments:
            resolution: the given spatial resolution
        """
        if CellBenchmarkConfigurationType.CELL == self.type \
                and resolution not in self.VALID_RESOLUTIONS:
            raise ValueError(f'Invalid spatial resolution <{resolution}> provided for CellBenchmarkConfiguration. '
                             'Only <{self.VALID_RESOLUTIONS}> are supported')

    def get_parameters(self) -> Dict[str, any]:
        """Return query parameters based on configuration."""
        return {
            'START_ID': self.start_date_id,
            'END_ID': self.end_date_id,
            'AREA_ID': self.enc_cell_id,
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


SINGLE_PARTITION_ID = 152
SMALL_AREA_ID = 117
MEDIUM_AREA_ID = 137
LARGE_AREA_ID = 148


class CellBenchmarkRunner(AbstractRuntimeBenchmarkRunner):
    """Benchmark runner to measure the runtime of cell benchmark queries."""

    QUERY_PREFIX = 'explain (analyze, timing, format json, verbose, buffers, settings)'

    def __init__(self) -> None:
        """Initialize a cell benchmark runner."""
        super().__init__(
            garbage_queries_folder='benchmarks/garbage_queries/cell',
            garbage_queries_per_iteration=10,
            iterations=10
        )
        self.queries_folder = 'benchmarks/queries/cell'

    def _get_benchmarks_to_run(self) -> Dict[str, Callable]:
        """Create the cell benchmarks to run."""
        benchmarks = {}
        queries = self._get_queries_in_folder(self.queries_folder)
        configurations = self.__get_benchmark_configurations()
        for query_filename, query in queries.items():
            if query_filename.startswith('cell'):
                cell_configurations = dict((k, v) for k, v in configurations.items()
                                           if v.type == CellBenchmarkConfigurationType.CELL)
                benchmarks.update(self.__configure_benchmark(cell_configurations, query))
            elif query_filename.startswith('trajectory'):
                trajectory_configurations = dict((k, v) for k, v in configurations.items()
                                                 if v.type == CellBenchmarkConfigurationType.TRAJECTORY)
                benchmarks.update(self.__configure_benchmark(trajectory_configurations, query))
            else:
                raise ValueError('Unkown query definition in CellBenchmarkRunner')
        return benchmarks

    def __configure_benchmark(self, configurations: Dict[str, CellBenchmarkConfiguration], query: str) \
            -> Dict[str, Callable[[], RuntimeBenchmarkResult]]:
        """
        Create benchmarks based on configurations.

        Arguments:
            configurations: configurations used to create benchmarks
            query: the benchmark query for running the benchmarks
        """
        configured_benchmarks = {}
        for name, config in configurations.items():
            params = config.get_parameters()
            benchmark_id = self._get_next_test_id()
            benchmark_query = config.format_query(query)
            benchmark_query = f'{self.QUERY_PREFIX} \n{benchmark_query}'

            # Default parameters to avoid copy be reference in lambda
            configured_benchmarks[name] = lambda benchmark_id=benchmark_id, params=params, benchmark_query=benchmark_query, name=name: \
                RuntimeBenchmarkResult(
                    *measure_time(lambda: (self._execute(text(benchmark_query), params=params))),
                    benchmark_id,
                    name
                )
        return configured_benchmarks

    def __get_benchmark_configurations(self) -> Dict[str, CellBenchmarkConfiguration]:
        """Get all configurations for this benchmark."""
        return {  # start_id, end_id, spatial_resolution, enc_id, ship_types
            'cell_1_day_50m_single_partition_cargo_ships': \
            CellBenchmarkConfiguration(20220110, 20220110, SINGLE_PARTITION_ID,
                                       ['Cargo'], CellBenchmarkConfigurationType.CELL, 50),
            'cell_1_day_200m_single_partition_cargo_ships': \
            CellBenchmarkConfiguration(20220110, 20220110, SINGLE_PARTITION_ID,
                                       ['Cargo'], CellBenchmarkConfigurationType.CELL, 200),
            'cell_1_day_1000m_single_partition_cargo_ships': \
            CellBenchmarkConfiguration(20220110, 20220110, SINGLE_PARTITION_ID,
                                       ['Cargo'], CellBenchmarkConfigurationType.CELL, 1000),
            'cell_1_day_50m_small_area_unique_cargo_ships': \
            CellBenchmarkConfiguration(20220110, 20220110, SMALL_AREA_ID,
                                       ['Cargo'], CellBenchmarkConfigurationType.CELL, 50),
            'cell_1_day_200m_medium_area_unique_cargo_ships': \
            CellBenchmarkConfiguration(20220110, 20220110, MEDIUM_AREA_ID,
                                       ['Cargo'], CellBenchmarkConfigurationType.CELL, 200),
            'cell_1_day_1000m_medium_area_unique_cargo_ships': \
            CellBenchmarkConfiguration(20220110, 20220110, MEDIUM_AREA_ID,
                                       ['Cargo'], CellBenchmarkConfigurationType.CELL, 1000),
            'cell_1_day_5000m_medium_area_unique_cargo_ships': \
            CellBenchmarkConfiguration(20220110, 20220110, MEDIUM_AREA_ID,
                                       ['Cargo'], CellBenchmarkConfigurationType.CELL, 5000),
            'cell_1_day_5000m_large_area_unique_cargo_ships': \
            CellBenchmarkConfiguration(20220110, 20220110, LARGE_AREA_ID,
                                       ['Cargo'], CellBenchmarkConfigurationType.CELL, 5000),
            'cell_30_day_50m_single_partition_area_unique_cargo_ships': \
            CellBenchmarkConfiguration(20220126, 20220224, SINGLE_PARTITION_ID,
                                       ['Cargo'], CellBenchmarkConfigurationType.CELL, 50),
            'cell_30_day_50m_small_area_unique_cargo_ships': \
            CellBenchmarkConfiguration(20220126, 20220224, SMALL_AREA_ID,
                                       ['Cargo'], CellBenchmarkConfigurationType.CELL, 50),
            'cell_30_day_200m_medium_area_unique_cargo_ships': \
            CellBenchmarkConfiguration(20220126, 20220224, MEDIUM_AREA_ID,
                                       ['Cargo'], CellBenchmarkConfigurationType.CELL, 200),
            'cell_30_day_200m_single_partition_area_unique_cargo_ships': \
            CellBenchmarkConfiguration(20220126, 20220224, SINGLE_PARTITION_ID,
                                       ['Cargo'], CellBenchmarkConfigurationType.CELL, 200),
            'cell_30_day_1000m_medium_area_unique_cargo_ships': \
            CellBenchmarkConfiguration(20220126, 20220224, MEDIUM_AREA_ID,
                                       ['Cargo'], CellBenchmarkConfigurationType.CELL, 1000),
            'cell_30_day_1000m_single_partition_area_unique_cargo_ships': \
            CellBenchmarkConfiguration(20220126, 20220224, SINGLE_PARTITION_ID,
                                       ['Cargo'], CellBenchmarkConfigurationType.CELL, 1000),
            'cell_30_day_5000m_medium_area_unique_cargo_ships': \
            CellBenchmarkConfiguration(20220126, 20220224, MEDIUM_AREA_ID,
                                       ['Cargo'], CellBenchmarkConfigurationType.CELL, 5000),
            'cell_30_day_5000m_large_area_unique_cargo_ships': \
            CellBenchmarkConfiguration(20220126, 20220224, LARGE_AREA_ID,
                                       ['Cargp'], CellBenchmarkConfigurationType.CELL, 5000),
            'cell_90_day_50m_single_partition_area_unique_cargo_ships': \
            CellBenchmarkConfiguration(20220101, 20220331, SINGLE_PARTITION_ID,
                                       ['Cargo'], CellBenchmarkConfigurationType.CELL, 50),
            'cell_90_day_50m_small_area_unique_cargo_ships': \
            CellBenchmarkConfiguration(20220101, 20220331, SMALL_AREA_ID,
                                       ['Cargo'], CellBenchmarkConfigurationType.CELL, 50),
            'cell_90_day_200m_medium_area_unique_cargo_ships': \
            CellBenchmarkConfiguration(20220101, 20220331, MEDIUM_AREA_ID,
                                       ['Cargo'], CellBenchmarkConfigurationType.CELL, 200),
            'cell_90_day_200m_single_partition_area_unique_cargo_ships': \
            CellBenchmarkConfiguration(20220101, 20220331, SINGLE_PARTITION_ID,
                                       ['Cargo'], CellBenchmarkConfigurationType.CELL, 200),
            'cell_90_day_1000m_medium_area_unique_cargo_ships': \
            CellBenchmarkConfiguration(20220101, 20220331, MEDIUM_AREA_ID,
                                       ['Cargo'], CellBenchmarkConfigurationType.CELL, 1000),
            'cell_90_day_1000m_single_partition_area_unique_cargo_ships': \
            CellBenchmarkConfiguration(20220101, 20220331, SINGLE_PARTITION_ID,
                                       ['Cargo'], CellBenchmarkConfigurationType.CELL, 1000),
            'cell_90_day_5000m_large_area_unique_cargo_ships': \
            CellBenchmarkConfiguration(20220101, 20220331, LARGE_AREA_ID,
                                       ['Cargo'], CellBenchmarkConfigurationType.CELL, 5000),
            'cell_90_day_5000m_medium_area_unique_cargo_ships': \
            CellBenchmarkConfiguration(20220101, 20220331, MEDIUM_AREA_ID,
                                       ['Cargo'], CellBenchmarkConfigurationType.CELL, 5000),

            'traj_1_day_large_area_unique_cargo_ships': \
            CellBenchmarkConfiguration(20220110, 20220110, LARGE_AREA_ID,
                                       ['Cargo'], CellBenchmarkConfigurationType.TRAJECTORY),
            'traj_1_day_medium_area_unique_cargo_ships': \
            CellBenchmarkConfiguration(20220110, 20220110, MEDIUM_AREA_ID,
                                       ['Cargo'], CellBenchmarkConfigurationType.TRAJECTORY),
            'traj_1_day_small_area_unique_cargo_ships': \
            CellBenchmarkConfiguration(20220110, 20220110, SMALL_AREA_ID,
                                       ['Cargo'], CellBenchmarkConfigurationType.TRAJECTORY),
            'traj_30_day_large_area_unique_cargo_ships': \
            CellBenchmarkConfiguration(20220126, 20220224, LARGE_AREA_ID,
                                       ['Cargo'], CellBenchmarkConfigurationType.TRAJECTORY),
            'traj_30_day_medium_area_unique_cargo_ships': \
            CellBenchmarkConfiguration(20220126, 20220224, MEDIUM_AREA_ID,
                                       ['Cargo'], CellBenchmarkConfigurationType.TRAJECTORY),
            'traj_30_day_small_area_unique_cargo_ships': \
            CellBenchmarkConfiguration(20220126, 20220224, SMALL_AREA_ID,
                                       ['Cargo'], CellBenchmarkConfigurationType.TRAJECTORY),
            'traj_90_day_large_area_unique_cargo_ships': \
            CellBenchmarkConfiguration(20220101, 20220331, LARGE_AREA_ID,
                                       ['Cargo'], CellBenchmarkConfigurationType.TRAJECTORY),
            'traj_90_day_medium_area_unique_cargo_ships': \
            CellBenchmarkConfiguration(20220101, 20220331, MEDIUM_AREA_ID,
                                       ['Cargo'], CellBenchmarkConfigurationType.TRAJECTORY),
            'traj_90_day_small_area_unique_cargo_ships': \
            CellBenchmarkConfiguration(20220101, 20220331, SMALL_AREA_ID,
                                       ['Cargo'], CellBenchmarkConfigurationType.TRAJECTORY),
        }
