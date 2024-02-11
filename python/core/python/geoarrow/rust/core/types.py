from __future__ import annotations

from typing import Literal, Protocol, Tuple, TypeVar
from ._rust import (
    PointArray,
    LineStringArray,
    PolygonArray,
    MultiPointArray,
    MultiLineStringArray,
    MultiPolygonArray,
    MixedGeometryArray,
    GeometryCollectionArray,
    ChunkedPointArray,
    ChunkedLineStringArray,
    ChunkedPolygonArray,
    ChunkedMultiPointArray,
    ChunkedMultiLineStringArray,
    ChunkedMultiPolygonArray,
    ChunkedMixedGeometryArray,
    ChunkedGeometryCollectionArray,
)

AreaMethodT = Literal["ellipsoidal", "euclidean", "spherical"]
"""Acceptable strings to be passed into the `method` parameter for
[`area`][geoarrow.rust.core.area] and
[`signed_area`][geoarrow.rust.core.signed_area].
"""

SimplifyMethodT = Literal["rdp", "vw", "vw_preserve"]
"""Acceptable strings to be passed into the `method` parameter for
[`simplify`][geoarrow.rust.core.simplify].
"""

SimplifyInputT = TypeVar(
    "SimplifyInputT",
    PointArray,
    LineStringArray,
    PolygonArray,
    MultiPointArray,
    MultiLineStringArray,
    MultiPolygonArray,
    ChunkedMultiPointArray,
    ChunkedLineStringArray,
    ChunkedPolygonArray,
    ChunkedMultiPointArray,
    ChunkedMultiLineStringArray,
    ChunkedMultiPolygonArray,
)
"""Known geoarrow-rust types for input into [`simplify`][geoarrow.rust.core.simplify].
"""

class ArrowArrayExportable(Protocol):
    """An Arrow or GeoArrow array from an Arrow producer (e.g. geoarrow.c or pyarrow)."""

    def __arrow_c_array__(
        self, requested_schema: object | None = None
    ) -> Tuple[object, object]:
        ...


class ArrowStreamExportable(Protocol):
    """An Arrow or GeoArrow ChunkedArray or Table from an Arrow producer (e.g. geoarrow.c or pyarrow)."""

    def __arrow_c_stream__(self, requested_schema: object | None = None) -> object:
        ...
