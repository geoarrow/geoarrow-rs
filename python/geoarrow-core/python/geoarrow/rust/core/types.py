from __future__ import annotations

from typing import Literal, Union
from .enums import CoordType, Dimension, Edges

# The top-level import doesn't work for docs interlinking
from pyproj.crs.crs import CRS

CRSInput = Union[CRS, str, dict, int]
"""Acceptable input for the CRS parameter.

This can be a `pyproj.CRS` object or anything that can be passed to
`pyproj.CRS.from_user_input()`.
"""

IntFloat = Union[int, float]


CoordTypeT = Literal["interleaved", "separated"]
"""Acceptable coord_type strings.
"""

DimensionT = Literal["XY", "XYZ", "XYM", "XYZM", "xy", "xyz", "xym", "xyzm"]
"""Acceptable dimension strings.
"""

EdgesT = Literal["andoyer", "karney", "spherical", "thomas", "vincenty"]
"""Acceptable edges strings.
"""


CoordTypeInput = Union[CoordType, CoordTypeT]
"""Acceptable coord_type input.
"""

DimensionInput = Union[Dimension, DimensionT]
"""Acceptable dimension input.
"""

EdgesInput = Union[Edges, EdgesT]
"""Acceptable edges input.
"""
