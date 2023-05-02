from typing import Any, Callable, Dict, List
from benchmarks.runners.abstract_runtime_benchmark_runner import AbstractRuntimeBenchmarkRunner
from benchmarks.decorators.benchmark import benchmark_class
from benchmarks.runners.abstract_benchmark_runner import BRT
from benchmarks.configurations.lazy_benchmark_configuration import LazyBenchmarkConfiguration
from benchmarks.dataclasses.runtime_benchmark_result import RuntimeBenchmarkResult
from etl.helper_functions import measure_time, get_first_query_in_file, extract_smart_date_id_from_date
from sqlalchemy import text
from datetime import datetime


@benchmark_class(name='LAZY')
class Lazy50mBenchmarkRunner(AbstractRuntimeBenchmarkRunner):
    """Benchmark runner to measure the runtime of lazy 50m benchmark queries."""

    def __init__(self) -> None:
        """Initialize lazy 50m benchmark runner."""
        super().__init__(
            'benchmarks/garbage_queries/dynamic_50m'
        )
        self._queries_folder = 'benchmarks/queries/dynamic_50m'

    def _get_benchmarks_to_run(self) -> Dict[str, Callable[[], BRT]]:
        """Create the cell benchmarks to run."""
        queries = self._get_queries_in_folder(self._queries_folder)

        benchmarks = {}
        for query_name, query in queries.items():
            configurations = self._create_configurations(query_name)
            for benchmark_name, configuration in configurations.items():
                run_id = self._get_next_test_id()
                benchmark_query = f'{self._query_prefix}\n {query}'
                q_parameters = configuration.get_parameters()
                benchmarks[benchmark_name] = \
                    lambda id=run_id, name=benchmark_name, query=benchmark_query, param=q_parameters: \
                    RuntimeBenchmarkResult(
                        *measure_time(lambda: self._conn.execute(text(query), parameters=param)),
                        id,
                        name
                    )

        return benchmarks

    @staticmethod
    def _calculate_configuration_name(area: str, duration: str, query_name: str) -> str:
        """
        Calculate the configuration name.

        Arguments:
            area: name of the area benchmarked
            duration: text representation of the duration benchmarked
            query_name: the filename of the query
        """
        return f'{query_name}_{area}_{duration}'

    def _create_configurations(self, query_file_name: str) -> Dict[str, LazyBenchmarkConfiguration]:
        """
        Create lazy 50m benchmark configurations.

        Arguments:
            query_file_name: file name of the query that is being configured
        """
        area_id_geom_map = {  # xmin, ymin, xmax, ymax
            111: (3990401.028030803, 3207418.9322399576, 4006203.35118328, 3218920.138335524),
            6: (4068764.0226458698, 3158829.822705281, 4070546.425170807, 3159780.700336136),
            12: (4171444.567003976, 3187256.2165073296, 4174494.3510139505, 3189256.9809509492),
            126: (3993510.1855945783, 3341819.0227817534, 4004818.196538742, 3349485.39323123),
            46: (3916762.466170692, 3286466.0121565587, 3918032.2848470374, 3287840.0084120547),
            105: (3774249.7169026784, 3349361.8573706066, 4117821.215225073, 3579328.1464176113)
        }
        area_id_name_map = {
            111: 'horsens_fjord',
            6: 'korsoer',
            12: 'oeresundsbroen',
            126: 'limfjorden',
            46: 'venoe_sund_sneavring',
            105: 'skagerak'
        }
        duration_name_timestamp_map = {  # start, end
            '1_day': (datetime(year=2021, month=5, day=4),
                      datetime(year=2021, month=5, day=4, hour=23, minute=59, second=59)),
            '1_week': (datetime(year=2021, month=6, day=21),
                       datetime(year=2021, month=6, day=28, hour=23, minute=59, second=59)),
            '1_month': (datetime(year=2021, month=10, day=1),
                        datetime(year=2021, month=10, day=31, hour=23, minute=59, second=59))
        }

        configurations = {}
        for duration_name, (start, end) in duration_name_timestamp_map.items():
            for area_id, area_name in area_id_name_map.items():
                name = self._calculate_configuration_name(
                    area_name, duration_name,
                    'lazy' if query_file_name.startswith('lazy') else 'stored'
                )
                xmin, ymin, xmax, ymax = (area_id_geom_map[area_id])
                configurations[name] = LazyBenchmarkConfiguration(
                    xmin, ymin, xmax, ymax, start, end
                )
        return configurations

    def _parameterise_garbage(self) -> Dict[str, Any]:
        parameter_randimization_query = get_first_query_in_file('benchmarks/queries/misc/random_bounds.sql')
        start_timestamp = datetime(year=2021, month=1, day=1)
        end_timestamp = datetime(year=2021, month=12, day=31)
        result_row = self._conn.execute(text(parameter_randimization_query), parameters={
            'period_start_timestamp': start_timestamp,
            'period_end_timestamp': end_timestamp
        }).fetchone()

        return result_row._asdict() | {
            'start_date_id': extract_smart_date_id_from_date(start_timestamp),
            'end_date_id': extract_smart_date_id_from_date(end_timestamp)
        }
