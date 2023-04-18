"""Module for running benchmarks."""
import argparse
import sys
from enum import Enum
from benchmarks.runners.cell_benchmark_runner import CellBenchmarkRunner


class BenchmarkType(Enum):
    """Enumeration defining available benchmarks."""

    ALL = 'ALL'
    CELL = 'CELL'

    def __str__(self) -> str:
        """Return enumeration value as string representation."""
        return self.value


def configure_arguments() -> argparse.Namespace:
    """Configure the program argument parser."""
    parser = argparse.ArgumentParser()
    parser.add_argument('--benchmark', help='Run specified benchmark', type=BenchmarkType, choices=list(BenchmarkType))

    return parser.parse_args()


def run_benchmark(args: argparse.Namespace) -> None:
    """
    Run benchmarks based on arguments.

    Arguments:
        args: program arguments
    """
    if args.benchmark == BenchmarkType.ALL:
        CellBenchmarkRunner().run_benchmark()
        # Add new benchmark runners here
    elif args.benchmark == BenchmarkType.CELL:
        CellBenchmarkRunner().run_benchmark()
    # Add new benchmark runners to the chain


def main(argv) -> None:
    """Entry point for running benchmarks."""
    args = configure_arguments()
    run_benchmark(args)


if __name__ == '__main__':
    main(sys.argv[1:])
