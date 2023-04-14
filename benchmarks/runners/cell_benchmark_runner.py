""""""
from benchmarks.benchmark_runner import AbstractBenchmarkRunner
from typing import Dict, Callable

class CellBenchmarkRunner(AbstractBenchmarkRunner):
    def __init__(self) -> None:
        super().__init__(garbage_queries_folder='benchmarks/garbage_queries/cell', garbage_queries_per_iteration=10, iterations=10)
    
    def _get_benchmarks_to_run(self) -> Dict[str, Callable]:
        pass