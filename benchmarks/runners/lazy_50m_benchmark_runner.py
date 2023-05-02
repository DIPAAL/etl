from typing import Callable, Dict, List
from benchmarks.runners.abstract_runtime_benchmark_runner import AbstractRuntimeBenchmarkRunner
from benchmarks.decorators.benchmark import benchmark_class
from benchmarks.runners.abstract_benchmark_runner import BRT
from benchmarks.configurations.lazy_benchmark_configuration import LazyBenchmarkConfiguration
from benchmarks.dataclasses.runtime_benchmark_result import RuntimeBenchmarkResult
from etl.helper_functions import measure_time
from sqlalchemy import text
from datetime import datetime

@benchmark_class(name='LAZY')
class Lazy50mBenchmarkRunner(AbstractRuntimeBenchmarkRunner):
    def __init__(self) -> None:
        super().__init__(
            'benchmarks/garbage_queries/dynamic_50m'
        )
        self._queries_folder = 'benchmarks/queries/dynamic_50m'
    
    def _get_benchmarks_to_run(self) -> Dict[str, Callable[[], BRT]]:
        queries = self._get_queries_in_folder(self._queries_folder)

        benchmarks = {}
        for query_name, query in queries.items():
            configurations = self._create_configurations(query_name)
            for benchmark_name, configuration in configurations.items():
                run_id = self._get_next_test_id()
                benchmark_query = f'{self._query_prefix}\n {query}'
                q_parameters = configuration.get_parameters()
                benchmarks[benchmark_name] = lambda id=run_id, name=benchmark_name, query=benchmark_query, param=q_parameters: \
                    RuntimeBenchmarkResult(
                        *measure_time(lambda: self._conn.execute(text(query), parameters=param)),
                        id,
                        name
                    )

        return benchmarks

    def _calculate_configuration_name(self, area: str, duration: str, query_name: str) -> str:
        return f'{query_name}_{area}_{duration}'

    def _create_configurations(self, query_file_name: str) -> Dict[str, LazyBenchmarkConfiguration]:
        area_id_geom_map = {  # xmin, ymin, xmax, ymax
            111: (3990401.028030803, 3207418.9322399576, 4006203.35118328, 3218920.138335524),
            6: (4068764.0226458698, 3158829.822705281, 4070546.425170807, 3159780.700336136, 1633811.8864262477),
            12: (4171444.567003976, 3187256.2165073296, 4174494.3510139505, 3189256.9809509492),
            126: (3993510.1855945783, 3341819.0227817534, 4004818.196538742, 3349485.39323123),
            38: (3885347.645682372, 3231581.738579133, 3887828.042069245, 3233262.654634314)
        }
        area_id_name_map = {
            111: 'horsens_fjord',
            6: 'korsoer',
            12: 'oeresundsbroen',
            126: 'limfjorden',
            38: 'hvide_sande'
        }
        duration_name_timestamp_map = {  # start, end
            '1_day': (datetime(year=2021, month=5, day=4), datetime(year=2021, month=5, day=4, hour=23, minute=59, second=59)),
            '1_week': (datetime(year=2021, month=6, day=21), datetime(year=2021, month=6, day=28, hour=23, minute=59, second=59)),
            '1_month': (datetime(year=2021, month=10, day=1), datetime(year=2021, month=10, day=31, hour=23, minute=59, second=59))
        }

        configurations = {}
        for duration_name, (start, end) in duration_name_timestamp_map.items():
            for area_id, area_name in area_id_name_map.items():
                name = self._calculate_configuration_name(
                    area_name, duration_name,
                    'lazy' if query_file_name.startswith('lazy') else 'stored'
                )
                xmin, ymin, xmax, ymax = area_id_geom_map[area_id]
                configurations[name] = LazyBenchmarkConfiguration(
                    xmin, ymin, xmax, ymax, start, end
                )
        return configurations
