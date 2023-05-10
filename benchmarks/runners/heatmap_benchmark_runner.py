"""Module containing the heatmap benchmark runner."""
from benchmarks.dataclasses.geolimits import GeoLimits
from benchmarks.runners.abstract_runtime_benchmark_runner import AbstractRuntimeBenchmarkRunner
from benchmarks.runners.abstract_benchmark_runner import BRT
from benchmarks.configurations.heatmap_benchmark_configuration import HeatmapBenchmarkConfiguration
from benchmarks.dataclasses.runtime_benchmark_result import RuntimeBenchmarkResult
from etl.helper_functions import wrap_with_retry_and_timing
from typing import Dict, List, Callable
from sqlalchemy import text
from benchmarks.decorators.benchmark import benchmark_class


@benchmark_class(name='HEATMAP')
class HeatmapBenchmarkRunner(AbstractRuntimeBenchmarkRunner):
    """Benchmark runner to measure the runtime of heatmap benchmark queries."""

    def __init__(self) -> None:
        """Initialize cell benchmark runner."""
        super().__init__()
        self._query_folder = 'benchmarks/queries/heatmap'

    def _get_benchmarks_to_run(self) -> Dict[str, Callable[[], BRT]]:
        """Create the cell benchmarks to run."""
        benchmarks = {}
        queries = self._get_queries_in_folder(self._query_folder)
        for query_file_name, query in queries.items():
            configurations = self._get_configurations(query_file_name)
            benchmarks.update(
                self._configure_benchmark(configurations, query)
            )
        return benchmarks

    def _configure_benchmark(self, configurations: Dict[str, HeatmapBenchmarkConfiguration], query: str) -> BRT:
        """
        Create benchmarks based on configurations.

        Arguments:
            configurations: configurations used to create benchmarks
            query: the benchmark query for running the benchmarks
        """
        configured_benchmarks = {}
        for conf_name, config in configurations.items():
            benchmark_id = wrap_with_retry_and_timing('Get next test id', lambda: self._get_next_test_id())
            params = config.get_parameters()
            benchmark_query = f'{self._query_prefix} \n{query}'
            configured_benchmarks[conf_name] = \
                lambda id=benchmark_id, params=params, query=benchmark_query, name=conf_name: \
                    self._execute_runtime_benchmark(id, params, query, name, 'heatmap')
        return configured_benchmarks

    def _get_configurations(self, query_file_name: str) -> Dict[str, HeatmapBenchmarkConfiguration]:  # noqa: C901
        """
        Get all configurations for this benchmark.

        Arguments:
            query_file_name: the file name of the query to configure
        """
        duration_map = {
            '1_day': (20210228, 20210228),
            '1_month': (20210601, 20210630),
            '1_year': (20210101, 20211231)
        }
        resolutions = {
            5000: 'very_low_resolution',
            1000: 'low_resolution',
            200: 'medium_resolution',
            50: 'high_resolution'
        }
        areas = {
            'aarhus': GeoLimits(4012045, 3243300, 4018200, 3250900),
            'storebaelt': GeoLimits(4032800, 3145500, 4078100, 3213300),
            'complete': GeoLimits(3480000, 2930000, 4495000, 3645000),
        }
        ship_types = [['Cargo', 'Pleasure', 'Fishing']]
        mobile_types = [['Class A', 'Class B']]
        file_name = query_file_name[:-4] if query_file_name.endswith('.sql') else query_file_name
        configurations = {}
        for duration_name, (start_date_id, end_date_id) in duration_map.items():
            for resolution, resolution_name in resolutions.items():
                if resolution in self._available_resolutions:
                    for area_name, geolimits in areas.items():
                        for ship_type_list in ship_types:
                            for mobile_type_list in mobile_types:
                                conf_name = self._create_configuration_name(file_name, duration_name, resolution_name,
                                                                            area_name, ship_type_list, mobile_type_list)
                                configurations[conf_name] = HeatmapBenchmarkConfiguration(start_date_id, end_date_id,
                                                                                          resolution, geolimits,
                                                                                          'count',
                                                                                          ship_type_list,
                                                                                          mobile_type_list)
        return configurations

    @staticmethod
    def _create_configuration_name(type: str, duration: str, resolution: str, area: str, ship_types: List[str],
                                   mobile_types: List[str]) -> str:
        """
        Create configuration name.

        Arguments:
            type: the type of the heatmap benchmarked
            duration: representation of the temporal span the benchmark queries
            resolution: representation of the resolution of the heatmap queried
            area: the name of the benchmarked area
            ship_types: the ship types used when benchmarking the configuration
            mobile_types: the mobile types used when benchmarking the configuration
        """
        separator = '_'
        ship_str = separator.join(ship_types).lower().replace(' ', separator)
        mobile_str = separator.join(mobile_types).lower().replace(' ', separator)
        return f'{duration}_{area}_{resolution}_{ship_str}_{mobile_str}_{type}_heatmap'
