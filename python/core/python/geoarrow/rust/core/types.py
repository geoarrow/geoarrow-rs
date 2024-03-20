from __future__ import annotations

from typing import Literal, Protocol, Tuple, TypeVar, Union
from ._rust import (
    Point,
    LineString,
    Polygon,
    MultiPoint,
    MultiLineString,
    MultiPolygon,
    Geometry,
    GeometryCollection,
    Rect,
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

try:
    import numpy as np
    from numpy.typing import NDArray

    ScalarType_co = TypeVar("ScalarType_co", bound=np.generic, covariant=True)

except ImportError:
    ScalarType_co = TypeVar("ScalarType_co", covariant=True)


IntFloat = Union[int, float]

AffineInputT = TypeVar(
    "AffineInputT",
    PointArray,
    LineStringArray,
    PolygonArray,
    MultiPointArray,
    MultiLineStringArray,
    MultiPolygonArray,
    MixedGeometryArray,
    GeometryCollectionArray,
    ChunkedPointArray,
    ChunkedMultiPointArray,
    ChunkedLineStringArray,
    ChunkedPolygonArray,
    ChunkedMultiPointArray,
    ChunkedMultiLineStringArray,
    ChunkedMultiPolygonArray,
    ChunkedMixedGeometryArray,
    ChunkedGeometryCollectionArray,
)
"""
Known geoarrow-rust types for input into
[`affine_transform`][geoarrow.rust.core.affine_transform].
"""

AffineTransform = Union[
    Tuple[IntFloat, IntFloat, IntFloat, IntFloat, IntFloat, IntFloat],
    Tuple[
        IntFloat,
        IntFloat,
        IntFloat,
        IntFloat,
        IntFloat,
        IntFloat,
        IntFloat,
        IntFloat,
        IntFloat,
    ],
    Tuple[IntFloat, ...],
]

AreaMethodT = Literal["ellipsoidal", "euclidean", "spherical"]
"""Acceptable strings to be passed into the `method` parameter for
[`area`][geoarrow.rust.core.area] and
[`signed_area`][geoarrow.rust.core.signed_area].
"""

NativeGeometryArrayT = Union[
    PointArray,
    LineStringArray,
    PolygonArray,
    MultiPointArray,
    MultiLineStringArray,
    MultiPolygonArray,
    MixedGeometryArray,
    GeometryCollectionArray,
]

NativeChunkedGeometryArrayT = Union[
    ChunkedPointArray,
    ChunkedLineStringArray,
    ChunkedPolygonArray,
    ChunkedMultiPointArray,
    ChunkedMultiLineStringArray,
    ChunkedMultiPolygonArray,
    ChunkedMixedGeometryArray,
    ChunkedGeometryCollectionArray,
]

LengthMethodT = Literal["ellipsoidal", "euclidean", "haversine", "vincenty"]
"""Acceptable strings to be passed into the `method` parameter for
[`length`][geoarrow.rust.core.length].
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
    ) -> Tuple[object, object]: ...


class ArrowStreamExportable(Protocol):
    """An Arrow or GeoArrow ChunkedArray or Table from an Arrow producer (e.g. geoarrow.c or pyarrow)."""

    def __arrow_c_stream__(self, requested_schema: object | None = None) -> object: ...


class GeoInterfaceProtocol(Protocol):
    """A scalar geometry that implements the Geo Interface protocol."""

    @property
    def __geo_interface__(self) -> dict: ...


class NumpyArrayProtocolf64(Protocol):
    """A scalar geometry that implements the Geo Interface protocol."""

    @property
    def __array__(self) -> NDArray[np.float64]: ...


ScalarGeometry = Union[
    GeoInterfaceProtocol,
    Point,
    LineString,
    Polygon,
    MultiPoint,
    MultiLineString,
    MultiPolygon,
    Geometry,
    GeometryCollection,
    Rect,
]

BroadcastGeometry = Union[
    ScalarGeometry, NativeGeometryArrayT, NativeChunkedGeometryArrayT
]
