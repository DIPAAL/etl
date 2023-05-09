"""Module containing the cell benchmark runner."""
from benchmarks.dataclasses.geolimits import GeoLimits
from benchmarks.runners.abstract_runtime_benchmark_runner import AbstractRuntimeBenchmarkRunner
from benchmarks.dataclasses.runtime_benchmark_result import RuntimeBenchmarkResult
from benchmarks.enumerations.cell_benchmark_configuration_type import CellBenchmarkConfigurationType
from benchmarks.configurations.cell_benchmark_configuration import CellBenchmarkConfiguration
from benchmarks.decorators.benchmark import benchmark_class
from typing import Dict, List, Tuple, Callable
from etl.helper_functions import measure_time, wrap_with_retry_and_timing
from sqlalchemy import text

SINGLE_PARTITION = 'single_partition'
SMALL_AREA = 'small_area'
MEDIUM_AREA = 'medium_area'
LARGE_AREA = 'large_area'


@benchmark_class(name='CELL')
class CellBenchmarkRunner(AbstractRuntimeBenchmarkRunner):
    """Benchmark runner to measure the runtime of cell benchmark queries."""

    def __init__(self) -> None:
        """Initialize a cell benchmark runner."""
        super().__init__()
        self._queries_folder = 'benchmarks/queries/cell'

    def _get_benchmarks_to_run(self) -> Dict[str, Callable]:
        """Create the cell benchmarks to run."""
        benchmarks = {}
        queries = self._get_queries_in_folder(self._queries_folder)
        configurations = self._get_benchmark_configurations()
        for query_filename, query in queries.items():
            if query_filename.startswith('cell'):
                cell_configurations = dict((k, v) for k, v in configurations.items()
                                           if v.type == CellBenchmarkConfigurationType.CELL)
                benchmarks.update(self._configure_benchmark(cell_configurations, query))
            elif query_filename.startswith('trajectory'):
                trajectory_configurations = dict((k, v) for k, v in configurations.items()
                                                 if v.type == CellBenchmarkConfigurationType.TRAJECTORY)
                benchmarks.update(self._configure_benchmark(trajectory_configurations, query))
            else:
                raise ValueError('Unkown query definition in CellBenchmarkRunner')
        return benchmarks

    def _configure_benchmark(self, configurations: Dict[str, CellBenchmarkConfiguration], query: str) \
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
            benchmark_id = wrap_with_retry_and_timing('Get next test id', lambda: self._get_next_test_id())
            benchmark_query = config.format_query(query)
            benchmark_query = f'{self._query_prefix} \n{benchmark_query}'

            # Default parameters to avoid copy by reference in lambda
            configured_benchmarks[name] = \
                lambda benchmark_id=benchmark_id, params=params, benchmark_query=benchmark_query, name=name: \
                RuntimeBenchmarkResult(
                    *measure_time(lambda: (self._conn.execute(text(benchmark_query), parameters=params))),
                    benchmark_id,
                    name,
                    'cell'
                )
        return configured_benchmarks

    def _get_benchmark_configurations(self) -> Dict[str, CellBenchmarkConfiguration]:
        """Get all configurations for this benchmark."""
        duration_map = {
            '1_day': (20210110, 20210111),
            '30_day': (20210126, 20210225),
            '90_day': (20210101, 20210401)
        }
        areas_from_resolution = {
            50: [SINGLE_PARTITION, SMALL_AREA],
            200: [SINGLE_PARTITION, MEDIUM_AREA],
            1000: [SINGLE_PARTITION, MEDIUM_AREA, LARGE_AREA],
            5000: [MEDIUM_AREA, LARGE_AREA]
        }
        area_name_to_area = {
            SINGLE_PARTITION: GeoLimits(3963950, 3124950, 3965950, 3128250),
            SMALL_AREA: GeoLimits(4012045, 3243300, 4018200, 3250900),
            MEDIUM_AREA: GeoLimits(4003600, 3381500, 4093500, 3443000),
            LARGE_AREA: GeoLimits(3551600, 2980550, 4427050, 3592300)
        }
        ship_types = ['Cargo']
        configurations = {}

        configurations.update(self._create_cell_configurations(duration_map, areas_from_resolution,
                                                               area_name_to_area, ship_types))
        configurations.update(self._create_trajectory_configurations(duration_map, area_name_to_area, ship_types))
        return configurations

    def _calc_configuration_name(self, conf_type: str, duration: str, area: str,
                                 ship_types: List[str], resolution: int = None) -> str:
        """
        Calculate the name of the configuration.

        Arguments:
            conf_type: the type of configuration the name is created for
            duration: representation of the temporal span the configuration benchmarks
            area: the name of the area used for the configuration
            ship_types: the list of types used when benchmarking the configuration
            resolution: the spatial resolution for the configuration (default: None)
        """
        separator = '_'
        ship_str = separator.join(ship_types).lower().replace(' ', separator)
        resolution_str = '' if resolution is None else f'{resolution}m'
        return f'{conf_type}_{duration}_{resolution_str}_{area}_unique_{ship_str}_ships'

    def _create_cell_configurations(self,
                                    duration_map: Dict[str, Tuple[int, int]],
                                    areas_from_resolution: Dict[int, List[str]],
                                    name_to_area: Dict[str, GeoLimits],
                                    ship_types: List[str]) \
            -> Dict[str, CellBenchmarkConfiguration]:
        """
        Create cell configurations.

        Arguments:
            duration_map: maps duration representations to start and end date values
            areas_from_resolution: maps spatial resolution to benchmarked area ids
            name_to_area: maps of area name to area limits
            ship_types: the list of types used when benchmarking the configuration
        """
        cell_configurations = {}
        for duration_name, (start_date_id, end_date_id) in duration_map.items():
            for s_resolution in self._available_resolutions:
                for area_name in areas_from_resolution[s_resolution]:
                    geolimits = name_to_area[area_name]
                    conf_name = self._calc_configuration_name(CellBenchmarkConfigurationType.CELL.value, duration_name,
                                                              area_name, ship_types, s_resolution)
                    cell_configurations[conf_name] = \
                        CellBenchmarkConfiguration(start_date_id, end_date_id, geolimits, ship_types,
                                                   CellBenchmarkConfigurationType.CELL, s_resolution)
        return cell_configurations

    def _create_trajectory_configurations(self,
                                          duration_map: Dict[str, Tuple[int, int]],
                                          area_name_to_area: Dict[str, GeoLimits],
                                          ship_types: List[str]) \
            -> Dict[str, CellBenchmarkConfiguration]:
        """
        Create trajectory configurations.

        Arguments:
            duration_map: maps duration representations to start and end date values
            area_name_to_area: maps of area name to area limits
            ship_types: the list of types used when benchmarking the configuration
        """
        trajectory_configurations = {}
        for duration_name, (start_date_id, end_date_id) in duration_map.items():
            for area_name, geolimits in area_name_to_area.items():
                conf_name = self._calc_configuration_name(CellBenchmarkConfigurationType.TRAJECTORY.value,
                                                          duration_name, area_name, ship_types)
                trajectory_configurations[conf_name] = \
                    CellBenchmarkConfiguration(start_date_id, end_date_id, geolimits, ship_types,
                                               CellBenchmarkConfigurationType.TRAJECTORY)

        return trajectory_configurations
