from __future__ import annotations

from typing import Literal, Protocol, Sequence, Tuple, TypeVar, TypedDict, Union
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

GeoParquetEncodingT = Literal["wkb", "native"]
"""Acceptable strings to be passed into the `encoding` parameter for
[`write_parquet`][geoarrow.rust.core.write_parquet].
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

RotateOriginT = Literal["center", "centroid"]
"""Acceptable strings to be passed into the `origin` parameter for
[`rotate`][geoarrow.rust.core.rotate].
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


class ArrowSchemaExportable(Protocol):
    """An Arrow or GeoArrow schema or field."""

    def __arrow_c_schema__(self) -> object: ...


class ArrowArrayExportable(Protocol):
    """An Arrow or GeoArrow array or RecordBatch."""

    def __arrow_c_array__(self, requested_schema) -> Tuple[object, object]: ...


class ArrowStreamExportable(Protocol):
    """An Arrow or GeoArrow ChunkedArray or Table."""

    def __arrow_c_stream__(self, requested_schema) -> object: ...


class GeoInterfaceProtocol(Protocol):
    """A scalar geometry that implements the Geo Interface protocol."""

    @property
    def __geo_interface__(self) -> dict: ...


class NumpyArrayProtocolf64(Protocol):
    """An object that implements the numpy __array__ method."""

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
    ScalarGeometry,
    NativeGeometryArrayT,
    NativeChunkedGeometryArrayT,
    ArrowArrayExportable,
    ArrowStreamExportable,
]


class BboxPaths(TypedDict):
    minx_path: Sequence[str]
    miny_path: Sequence[str]
    maxx_path: Sequence[str]
    maxy_path: Sequence[str]
