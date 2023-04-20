"""Module storing enumeration for cell benchmark configurations."""
from enum import Enum


class CellBenchmarkConfigurationType(Enum):
    """Enumeration defining possible cell benchmark configuration types."""

    CELL = 'cell'
    TRAJECTORY = 'traj'

    def __str__(self) -> str:
        """Return enumeration value as string representation."""
        return self.value
