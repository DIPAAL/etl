"""DataClass definition for GeoLimits."""
from dataclasses import dataclass


@dataclass
class GeoLimits:
    """Class for storing x and y limits of a geometric rectangle."""

    xmin: int
    ymin: int
    xmax: int
    ymax: int
