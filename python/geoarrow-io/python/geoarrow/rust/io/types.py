from __future__ import annotations

from typing import Literal


GeoParquetEncodingT = Literal["wkb", "geoarrow"]
"""Acceptable strings to be passed into the `encoding` parameter for
[`GeoParquetWriter`][geoarrow.rust.io.GeoParquetWriter].
"""
