from __future__ import annotations

from typing import Literal, Sequence, TypedDict, Union

IntFloat = Union[int, float]


GeoParquetEncodingT = Literal["wkb", "native"]
"""Acceptable strings to be passed into the `encoding` parameter for
[`write_parquet`][geoarrow.rust.core.write_parquet].
"""


class BboxPaths(TypedDict):
    xmin: Sequence[str]
    ymin: Sequence[str]
    xmax: Sequence[str]
    ymax: Sequence[str]
