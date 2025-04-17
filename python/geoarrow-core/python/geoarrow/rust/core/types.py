from __future__ import annotations

from typing import Union

# The top-level import doesn't work for docs interlinking
from pyproj.crs.crs import CRS

CRSInput = Union[CRS, str, dict, int]
"""Acceptable input for the CRS parameter."""

IntFloat = Union[int, float]
