"""Module consisting of wrappers for dynamically adding runable benchmarks to the program."""
from benchmarks.runners.abstract_benchmark_runner import AbstractBenchmarkRunner
from typing import Type, List, Dict, Callable


registered_benchmarks: Dict[str, Callable[[], None]] = {}
ALL_BENCHMARKS_KEY = 'ALL'


def benchmark_class(cls: Type[AbstractBenchmarkRunner] = None, *, name: str,
                    init_args: List | None = None, init_kwargs: Dict | None = None):
    """
    Register benchmark to be run.

    Arguments:
        cls: the benchmark runner class to wrap (default: None)
        name: the name of the benchmark
        init_args: positional arguments for the benchmark runner constructor (default: [])
        init_kwargs: keyword arguments for the benchmark runner constructor (default: {})
    """
    def wrap(cls: Type[AbstractBenchmarkRunner]):
        runner = cls.__new__(cls)
        runner.__init__(
            *([] if init_args is None else init_args), 
            **({} if init_kwargs is None else init_kwargs)
        )
        registered_benchmarks[name.upper()] = runner.run_benchmark
        return runner

    if cls is None:
        return wrap
    return wrap(cls)


def get_registered_benchmarks() -> List[str]:
    """Get the names of the registered benchmarks."""
    return [ALL_BENCHMARKS_KEY] + list(registered_benchmarks.keys())


def run_benchmark(benchmark: str) -> None:
    """
    Run benchmarks.

    Special value 'ALL' used to run all registered benchmarks.

    Arguments:
        benchmark: registered name of the benchmark to run
    """
    if benchmark == ALL_BENCHMARKS_KEY:
        for _, benchmark_exec in registered_benchmarks.items():
            benchmark_exec()
    else:
        benchmark_exec = registered_benchmarks[benchmark]
        benchmark_exec()
