"""Module containing the cell benchmark runner."""
from benchmarks.runners.abstract_runtime_benchmark_runner import AbstractRuntimeBenchmarkRunner
from benchmarks.dataclasses.runtime_benchmark_result import RuntimeBenchmarkResult
from benchmarks.enumerations.cell_benchmark_configuration_type import CellBenchmarkConfigurationType
from benchmarks.configurations.cell_benchmark_configuration import CellBenchmarkConfiguration
from benchmarks.decorators.benchmark import benchmark_class
from typing import Any, Dict, List, Tuple, Callable
from etl.helper_functions import measure_time, get_first_query_in_file
from sqlalchemy import text
from datetime import datetime

SINGLE_PARTITION_ID = 152
SMALL_AREA_ID = 117
MEDIUM_AREA_ID = 137
LARGE_AREA_ID = 148


@benchmark_class(name='CELL')
class CellBenchmarkRunner(AbstractRuntimeBenchmarkRunner):
    """Benchmark runner to measure the runtime of cell benchmark queries."""

    def __init__(self) -> None:
        """Initialize a cell benchmark runner."""
        super().__init__(
            garbage_queries_folder='benchmarks/garbage_queries/cell'
        )
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
            benchmark_id = self._get_next_test_id()
            benchmark_query = config.format_query(query)
            benchmark_query = f'{self._query_prefix} \n{benchmark_query}'

            # Default parameters to avoid copy by reference in lambda
            configured_benchmarks[name] = \
                lambda benchmark_id=benchmark_id, params=params, benchmark_query=benchmark_query, name=name: \
                RuntimeBenchmarkResult(
                    *measure_time(lambda: (self._conn.execute(text(benchmark_query), parameters=params))),
                    benchmark_id,
                    name
                )
        return configured_benchmarks

    def _get_benchmark_configurations(self) -> Dict[str, CellBenchmarkConfiguration]:
        """Get all configurations for this benchmark."""
        duration_map = {
            '1_day': (20210110, 20210110),
            '30_day': (20210126, 20210224),
            '90_day': (20210101, 20210331)
        }
        areas_from_resolution = {
            50: [SINGLE_PARTITION_ID, SMALL_AREA_ID],
            200: [SINGLE_PARTITION_ID, MEDIUM_AREA_ID],
            1000: [SINGLE_PARTITION_ID, MEDIUM_AREA_ID, LARGE_AREA_ID],
            5000: [MEDIUM_AREA_ID, LARGE_AREA_ID]
        }
        area_id_to_name = {
            SINGLE_PARTITION_ID: 'single_partition',
            SMALL_AREA_ID: 'small_area',
            MEDIUM_AREA_ID: 'medium_area',
            LARGE_AREA_ID: 'large_area'
        }
        ship_types = ['Cargo']
        configurations = {}

        configurations.update(self._create_cell_configurations(duration_map, areas_from_resolution,
                                                               area_id_to_name, ship_types))
        configurations.update(self._create_trajectory_configurations(duration_map, area_id_to_name, ship_types))
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
                                    areas_from_resolution: Dict[int, List[int]],
                                    area_id_to_name: Dict[int, str],
                                    ship_types: List[str]) \
            -> Dict[str, CellBenchmarkConfiguration]:
        """
        Create cell configurations.

        Arguments:
            duration_map: maps duration representations to start and end date values
            areas_from_resolution: maps spatial resolution to benchmarked area ids
            area_id_to_name: maps of area id to area name
            ship_types: the list of types used when benchmarking the configuration
        """
        cell_configurations = {}
        for duration_name, (start_date_id, end_date_id) in duration_map.items():
            for s_resolution in self._available_resolutions:
                for area_id in areas_from_resolution[s_resolution]:
                    area_name = area_id_to_name[area_id]
                    conf_name = self._calc_configuration_name(CellBenchmarkConfigurationType.CELL, duration_name,
                                                              area_name, ship_types, s_resolution)
                    cell_configurations[conf_name] = \
                        CellBenchmarkConfiguration(start_date_id, end_date_id, area_id, ship_types,
                                                   CellBenchmarkConfigurationType.CELL, s_resolution)
        return cell_configurations

    def _create_trajectory_configurations(self,
                                          duration_map: Dict[str, Tuple[int, int]],
                                          area_id_to_name: Dict[int, str],
                                          ship_types: List[str]) \
            -> Dict[str, CellBenchmarkConfiguration]:
        """
        Create trajectory configurations.

        Arguments:
            duration_map: maps duration representations to start and end date values
            area_id_to_name: maps of area id to area name
            ship_types: the list of types used when benchmarking the configuration
        """
        trajectory_configurations = {}
        for duration_name, (start_date_id, end_date_id) in duration_map.items():
            for area_id, area_name in area_id_to_name.items():
                conf_name = self._calc_configuration_name(CellBenchmarkConfigurationType.TRAJECTORY, duration_name,
                                                          area_name, ship_types)
                trajectory_configurations[conf_name] = \
                    CellBenchmarkConfiguration(start_date_id, end_date_id, area_id, ship_types,
                                               CellBenchmarkConfigurationType.TRAJECTORY)

        return trajectory_configurations

    def _parameterise_garbage(self) -> Dict[str, Any]:
        """Create parameters for garbage query."""
        random_bounds_query = get_first_query_in_file('benchmarks/queries/misc/random_bounds.sql')
        result_row = self._conn.execute(text(random_bounds_query), parameters={
            'period_start_timestamp': datetime(year=2021, month=1, day=1),
            'period_end_timestamp': datetime(year=2021, month=12, day=31)
        }).fetchone()
        return result_row._asdict()
