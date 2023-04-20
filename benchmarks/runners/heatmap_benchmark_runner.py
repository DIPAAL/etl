""""""
from benchmarks.runners.abstract_runtime_benchmark_runner import AbstractRuntimeBenchmarkRunner
from benchmarks.runners.abstract_benchmark_runner import BRT
from benchmarks.configurations.heatmap_benchmark_configuration import HeatmapBenchmarkConfiguration
from benchmarks.dataclasses.runtime_benchmark_result import RuntimeBenchmarkResult
from etl.helper_functions import measure_time, flatten_string_list
from typing import Dict, List, Callable
from sqlalchemy import text
from benchmarks.decorators.benchmark import benchmark_class

@benchmark_class(name='HEATMAP')
class HeatmapBenchmarkRunner(AbstractRuntimeBenchmarkRunner):
    """"""

    def __init__(self) -> None:
        """"""
        super().__init__(
            garbage_queries_folder='benchmarks/garbage_queries/heatmap',
            garbage_queries_per_iteration=4,
            iterations=2)
        self.query_folder = 'benchmarks/queries/heatmap'

    def _get_benchmarks_to_run(self) -> Dict[str, Callable[[], BRT]]:
        """"""
        benchmarks = {}
        queries = self._get_queries_in_folder(self.query_folder)
        for query_file_name, query in queries.items():
            configurations = self._get_configurations(query_file_name)
            benchmarks.update(
                self.__configure_benchmark(configurations, query)
            )
        return benchmarks

    def __configure_benchmark(self, configurations: Dict[str, HeatmapBenchmarkConfiguration], query: str) -> BRT:
        """"""
        configured_benchmarks = {}
        for conf_name, config in configurations.items():
            benchmark_id = self._get_next_test_id()
            params = config.get_parameters()
            benchmark_query = f'{self._query_prefix} \n{query}'
            configured_benchmarks[conf_name] = \
                lambda id=benchmark_id, params=params, benchmark_query=benchmark_query, benchmark_name=conf_name: \
                 RuntimeBenchmarkResult(
                    *measure_time(lambda: self._conn.execute(text(benchmark_query), parameters=params)),
                    id,
                    benchmark_name
                 )
        return configured_benchmarks


    def _get_configurations(self, query_file_name: str) -> Dict[str, HeatmapBenchmarkConfiguration]:
        """"""
        duration_map = {
            '1_day': (20220228, 20220228),
            '1_month': (20220601, 20220630),
            '1_year': (20220101, 20221231)
        }
        resolutions = {
            5000: 'low_resolution'
        }
        areas = {
            117: 'denmark',
            95: 'storebealt',
            148: 'whole_denmark'
        }
        ship_types = [['Cargo'], ['Pleasure']]
        mobile_types= [['Class A'], ['Class B'], ['Class A', 'Class B']]
        file_name = query_file_name[:-4] if query_file_name.endswith('.sql') else query_file_name
        configurations = {}
        for duration_name, (start_date_id, end_date_id) in duration_map.items():
            for resolution, resolution_name in resolutions.items():
                for area_id, area_name in areas.items():
                    for ship_type_list in ship_types:
                        for mobile_type_list in mobile_types:
                            conf_name = self._create_configuration_name(file_name, duration_name, resolution_name, area_name, ship_type_list, mobile_type_list)
                            configurations[conf_name] = HeatmapBenchmarkConfiguration(start_date_id, end_date_id, resolution, area_id, 'count', ship_type_list, mobile_type_list)
        return configurations


    def _create_configuration_name(self, type: str, duration: str, resolution: str, area: str, ship_types: List[str], mobile_types: List[str]) -> str:
        """"""
        ship_str = flatten_string_list(ship_types)
        mobile_str = flatten_string_list(mobile_types)
        return f'{duration}_{area}_{resolution}{ship_str}{mobile_str}_{type}_heatmap'
