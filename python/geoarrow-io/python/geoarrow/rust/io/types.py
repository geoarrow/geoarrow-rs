from __future__ import annotations

from typing import Literal


GeoParquetEncodingT = Literal["wkb", "native"]
"""Acceptable strings to be passed into the `encoding` parameter for
[`write_parquet`][geoarrow.rust.io.write_parquet].
"""
