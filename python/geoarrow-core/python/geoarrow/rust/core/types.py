from __future__ import annotations

from typing import Literal, Union


IntFloat = Union[int, float]


CoordTypeT = Literal["interleaved", "separated"]
"""Acceptable coord_type strings.
"""

DimensionT = Literal["XY", "XYZ", "xy", "xyz"]
"""Acceptable dimension strings.
"""
