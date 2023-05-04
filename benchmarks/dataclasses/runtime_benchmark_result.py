"""Module containing data class for runtime benchmark results."""
from dataclasses import dataclass
from sqlalchemy import CursorResult


@dataclass
class RuntimeBenchmarkResult:
    """Dataclass for storing the result of a runtime benchmark."""

    result: CursorResult
    time_taken: float
    benchmark_id: int
    benchmark_name: str
    benchmark_type: str
