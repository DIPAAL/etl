"""Module for running benchmarks."""
import argparse
import sys
from enum import Enum
from benchmarks.decorators.benchmark import get_registered_benchmarks, run_benchmark as rb
from benchmarks.runners import *  # noqa: F403,F401


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
    parser.add_argument('--benchmark', help='Run specified benchmark', required=True,
                        type=str, choices=get_registered_benchmarks())

    return parser.parse_args()


def run_benchmark(args: argparse.Namespace) -> None:
    """
    Run benchmarks based on arguments.

    Arguments:
        args: program arguments
    """
    rb(args.benchmark)


def main(argv) -> None:
    """Entry point for running benchmarks."""
    args = configure_arguments()
    run_benchmark(args)


if __name__ == '__main__':
    main(sys.argv[1:])
