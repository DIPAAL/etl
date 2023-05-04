"""Module containing the heatmap benchmark runner."""
from benchmarks.runners.abstract_runtime_benchmark_runner import AbstractRuntimeBenchmarkRunner
from benchmarks.runners.abstract_benchmark_runner import BRT
from benchmarks.configurations.heatmap_benchmark_configuration import HeatmapBenchmarkConfiguration
from benchmarks.dataclasses.runtime_benchmark_result import RuntimeBenchmarkResult
from etl.helper_functions import measure_time, get_first_query_in_file, extract_smart_date_id_from_date
from typing import Dict, List, Callable, Any
from sqlalchemy import text
from benchmarks.decorators.benchmark import benchmark_class
from datetime import datetime


@benchmark_class(name='HEATMAP')
class HeatmapBenchmarkRunner(AbstractRuntimeBenchmarkRunner):
    """Benchmark runner to measure the runtime of heatmap benchmark queries."""

    def __init__(self) -> None:
        """Initialize cell benchmark runner."""
        super().__init__(
            garbage_queries_folder='benchmarks/garbage_queries/heatmap'
        )
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
            benchmark_id = self._get_next_test_id()
            params = config.get_parameters()
            configured_benchmarks[conf_name] = \
                lambda id=benchmark_id, params=params, benchmark_query=query, benchmark_name=conf_name: \
                RuntimeBenchmarkResult(
                    *measure_time(lambda: self._conn.execute(text(benchmark_query), parameters=params)),
                    id,
                    benchmark_name
                 )
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
            117: 'denmark',
            95: 'storebealt',
            148: 'whole_denmark'
        }
        ship_types = [['Cargo', 'Pleasure', 'Fishing']]
        mobile_types = [['Class A', 'Class B']]
        file_name = query_file_name[:-4] if query_file_name.endswith('.sql') else query_file_name
        configurations = {}
        for duration_name, (start_date_id, end_date_id) in duration_map.items():
            for resolution, resolution_name in resolutions.items():
                if resolution in self._available_resolutions:
                    for area_id, area_name in areas.items():
                        for ship_type_list in ship_types:
                            for mobile_type_list in mobile_types:
                                conf_name = self._create_configuration_name(file_name, duration_name, resolution_name,
                                                                            area_name, ship_type_list, mobile_type_list)
                                configurations[conf_name] = HeatmapBenchmarkConfiguration(start_date_id, end_date_id,
                                                                                          resolution, area_id, 'count',
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

    def _store_result(self, iteration: int, result: RuntimeBenchmarkResult) -> None:
        """
        Store the result of running the benchmark.

        Arguments:
            iteration: the iteration of the benchmark
            result: all the results of running the particular benchmark
        """
        data = f'{{"Runtime_ms": {result.time_taken}}}'
        self._conn.execute(
            text("""
                INSERT INTO benchmark_results
                (test_run_id, query_name, iteration, explain, execution_time_ms)
                VALUES (:id, :name, :it, :result, :time)
            """),
            {'id': result.benchmark_id, 'name': result.benchmark_name,
             'it': iteration, 'result': data, 'time': result.time_taken}
        )

    def _parameterise_garbage(self) -> Dict[str, Any]:
        """Create parameters for garbage query."""
        random_bounds_query = get_first_query_in_file('benchmarks/queries/misc/random_bounds.sql')
        start_timestamp = datetime(year=2021, month=1, day=1)
        end_timestamp = datetime(year=2021, month=12, day=31)
        result_row = self._conn.execute(text(random_bounds_query), parameters={
            'period_start_timestamp': start_timestamp,
            'period_end_timestamp': end_timestamp
        }).fetchone()

        return result_row._asdict() | {
            'start_date_id': extract_smart_date_id_from_date(start_timestamp),
            'end_date_id': extract_smart_date_id_from_date(end_timestamp)
        }
