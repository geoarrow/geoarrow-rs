from __future__ import annotations

from typing import Literal, Union

import pyproj

CRSInput = Union[pyproj.CRS, str, dict, int]
"""Acceptable input for the CRS parameter."""

IntFloat = Union[int, float]


CoordTypeT = Literal["interleaved", "separated"]
"""Acceptable coord_type strings.
"""

DimensionT = Literal["XY", "XYZ", "xy", "xyz"]
"""Acceptable dimension strings.
"""
