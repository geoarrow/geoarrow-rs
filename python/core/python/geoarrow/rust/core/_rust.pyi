from __future__ import annotations

from pathlib import Path
from typing import (
    BinaryIO,
    Dict,
    List,
    Optional,
    Self,
    Sequence,
    Tuple,
    TypeVar,
    Union,
    overload,
)

from arro3.core import Array, ChunkedArray, RecordBatchReader, Table

try:
    import numpy as np
    from numpy.typing import NDArray
except ImportError:
    pass

try:
    import geopandas as gpd
except ImportError:
    pass

from .enums import AreaMethod, GeoParquetEncoding, LengthMethod, SimplifyMethod
from .types import (
    AffineInputT,
    AffineTransform,
    AreaMethodT,
    ArrowArrayExportable,
    ArrowSchemaExportable,
    ArrowStreamExportable,
    BboxPaths,
    BroadcastGeometry,
    GeoInterfaceProtocol,
    GeoParquetEncodingT,
    IntFloat,
    LengthMethodT,
    NativeChunkedGeometryArrayT,
    NativeGeometryArrayT,
    NumpyArrayProtocolf64,
    SimplifyInputT,
    SimplifyMethodT,
)

class Point:
    def __arrow_c_array__(
        self, requested_schema: object | None = None
    ) -> Tuple[object, object]: ...
    def __eq__(self, other: Self) -> bool: ...
    @property
    def __geo_interface__(self) -> dict: ...
    def __repr__(self) -> str: ...
    def _repr_svg_(self) -> str: ...

class LineString:
    def __arrow_c_array__(
        self, requested_schema: object | None = None
    ) -> Tuple[object, object]: ...
    def __eq__(self, other: Self) -> bool: ...
    @property
    def __geo_interface__(self) -> dict: ...
    def __repr__(self) -> str: ...
    def _repr_svg_(self) -> str: ...

class Polygon:
    def __arrow_c_array__(
        self, requested_schema: object | None = None
    ) -> Tuple[object, object]: ...
    def __eq__(self, other: Self) -> bool: ...
    @property
    def __geo_interface__(self) -> dict: ...
    def __repr__(self) -> str: ...
    def _repr_svg_(self) -> str: ...

class MultiPoint:
    def __arrow_c_array__(
        self, requested_schema: object | None = None
    ) -> Tuple[object, object]: ...
    def __eq__(self, other: Self) -> bool: ...
    @property
    def __geo_interface__(self) -> dict: ...
    def __repr__(self) -> str: ...
    def _repr_svg_(self) -> str: ...

class MultiLineString:
    def __arrow_c_array__(
        self, requested_schema: object | None = None
    ) -> Tuple[object, object]: ...
    def __eq__(self, other: Self) -> bool: ...
    @property
    def __geo_interface__(self) -> dict: ...
    def __repr__(self) -> str: ...
    def _repr_svg_(self) -> str: ...

class MultiPolygon:
    def __arrow_c_array__(
        self, requested_schema: object | None = None
    ) -> Tuple[object, object]: ...
    def __eq__(self, other: Self) -> bool: ...
    @property
    def __geo_interface__(self) -> dict: ...
    def __repr__(self) -> str: ...
    def _repr_svg_(self) -> str: ...

class Geometry:
    # def __arrow_c_array__(
    #     self, requested_schema: object | None = None
    # ) -> Tuple[object, object]: ...
    def __eq__(self, other: Self) -> bool: ...
    @property
    def __geo_interface__(self) -> dict: ...
    def __repr__(self) -> str: ...
    def _repr_svg_(self) -> str: ...

class GeometryCollection:
    def __arrow_c_array__(
        self, requested_schema: object | None = None
    ) -> Tuple[object, object]: ...
    def __eq__(self, other: Self) -> bool: ...
    @property
    def __geo_interface__(self) -> dict: ...
    def __repr__(self) -> str: ...
    def _repr_svg_(self) -> str: ...

class WKB:
    def __arrow_c_array__(
        self, requested_schema: object | None = None
    ) -> Tuple[object, object]: ...
    def __eq__(self, other: Self) -> bool: ...
    def __repr__(self) -> str: ...

class Rect:
    def __arrow_c_array__(
        self, requested_schema: object | None = None
    ) -> Tuple[object, object]: ...
    def __eq__(self, other: Self) -> bool: ...
    def __repr__(self) -> str: ...

class PointArray:
    def __eq__(self, other: Self) -> bool: ...
    @property
    def __geo_interface__(self) -> dict: ...
    def __getitem__(self, key: int) -> Point: ...
    def __len__(self) -> int: ...
    def __arrow_c_array__(
        self, requested_schema: object | None = None
    ) -> Tuple[object, object]: ...
    def __repr__(self) -> str: ...
    @classmethod
    def from_xy(
        cls,
        x: ArrowArrayExportable | NumpyArrayProtocolf64,
        y: ArrowArrayExportable | NumpyArrayProtocolf64,
    ) -> Self: ...
    def total_bounds(self) -> Tuple[float, float, float, float]: ...
    @classmethod
    def from_arrow(cls, input: ArrowArrayExportable) -> Self: ...
    @classmethod
    def from_shapely(cls, input) -> Self: ...
    @classmethod
    def from_wkb(cls, input: ArrowArrayExportable) -> Self: ...
    def to_shapely(self) -> NDArray[np.object_]: ...
    def to_wkb(self) -> WKBArray: ...

class LineStringArray:
    def __eq__(self, other: Self) -> bool: ...
    @property
    def __geo_interface__(self) -> dict: ...
    def __getitem__(self, key: int) -> LineString: ...
    def __len__(self) -> int: ...
    def __arrow_c_array__(
        self, requested_schema: object | None = None
    ) -> Tuple[object, object]: ...
    def __repr__(self) -> str: ...
    def total_bounds(self) -> Tuple[float, float, float, float]: ...
    @classmethod
    def from_arrow(cls, input: ArrowArrayExportable) -> Self: ...
    @classmethod
    def from_shapely(cls, input) -> Self: ...
    @classmethod
    def from_wkb(cls, input: ArrowArrayExportable) -> Self: ...
    def to_shapely(self) -> NDArray[np.object_]: ...
    def to_wkb(self) -> WKBArray: ...

class PolygonArray:
    def __eq__(self, other: Self) -> bool: ...
    @property
    def __geo_interface__(self) -> dict: ...
    def __getitem__(self, key: int) -> Polygon: ...
    def __len__(self) -> int: ...
    def __arrow_c_array__(
        self, requested_schema: object | None = None
    ) -> Tuple[object, object]: ...
    def __repr__(self) -> str: ...
    def polylabel(self, tolerance: float) -> PointArray: ...
    def total_bounds(self) -> Tuple[float, float, float, float]: ...
    @classmethod
    def from_arrow(cls, input: ArrowArrayExportable) -> Self: ...
    @classmethod
    def from_shapely(cls, input) -> Self: ...
    @classmethod
    def from_wkb(cls, input: ArrowArrayExportable) -> Self: ...
    def to_shapely(self) -> NDArray[np.object_]: ...
    def to_wkb(self) -> WKBArray: ...

class MultiPointArray:
    def __eq__(self, other: Self) -> bool: ...
    @property
    def __geo_interface__(self) -> dict: ...
    def __getitem__(self, key: int) -> MultiPoint: ...
    def __len__(self) -> int: ...
    def __arrow_c_array__(
        self, requested_schema: object | None = None
    ) -> Tuple[object, object]: ...
    def __repr__(self) -> str: ...
    def total_bounds(self) -> Tuple[float, float, float, float]: ...
    @classmethod
    def from_arrow(cls, input: ArrowArrayExportable) -> Self: ...
    @classmethod
    def from_shapely(cls, input) -> Self: ...
    @classmethod
    def from_wkb(cls, input: ArrowArrayExportable) -> Self: ...
    def to_shapely(self) -> NDArray[np.object_]: ...
    def to_wkb(self) -> WKBArray: ...

class MultiLineStringArray:
    def __eq__(self, other: Self) -> bool: ...
    @property
    def __geo_interface__(self) -> dict: ...
    def __getitem__(self, key: int) -> MultiLineString: ...
    def __len__(self) -> int: ...
    def __arrow_c_array__(
        self, requested_schema: object | None = None
    ) -> Tuple[object, object]: ...
    def __repr__(self) -> str: ...
    def total_bounds(self) -> Tuple[float, float, float, float]: ...
    @classmethod
    def from_arrow(cls, input: ArrowArrayExportable) -> Self: ...
    @classmethod
    def from_shapely(cls, input) -> Self: ...
    @classmethod
    def from_wkb(cls, input: ArrowArrayExportable) -> Self: ...
    def to_shapely(self) -> NDArray[np.object_]: ...
    def to_wkb(self) -> WKBArray: ...

class MultiPolygonArray:
    def __eq__(self, other: Self) -> bool: ...
    @property
    def __geo_interface__(self) -> dict: ...
    def __getitem__(self, key: int) -> MultiPolygon: ...
    def __len__(self) -> int: ...
    def __arrow_c_array__(
        self, requested_schema: object | None = None
    ) -> Tuple[object, object]: ...
    def __repr__(self) -> str: ...
    def total_bounds(self) -> Tuple[float, float, float, float]: ...
    @classmethod
    def from_arrow(cls, input: ArrowArrayExportable) -> Self: ...
    @classmethod
    def from_shapely(cls, input) -> Self: ...
    @classmethod
    def from_wkb(cls, input: ArrowArrayExportable) -> Self: ...
    def to_shapely(self) -> NDArray[np.object_]: ...
    def to_wkb(self) -> WKBArray: ...

class MixedGeometryArray:
    def __eq__(self, other: Self) -> bool: ...
    @property
    def __geo_interface__(self) -> dict: ...
    def __getitem__(self, key: int) -> Geometry: ...
    def __len__(self) -> int: ...
    def __arrow_c_array__(
        self, requested_schema: object | None = None
    ) -> Tuple[object, object]: ...
    def __repr__(self) -> str: ...
    def total_bounds(self) -> Tuple[float, float, float, float]: ...
    @classmethod
    def from_arrow(cls, input: ArrowArrayExportable) -> Self: ...
    @classmethod
    def from_ewkb(cls, input: ArrowArrayExportable) -> Self: ...
    @classmethod
    def from_shapely(cls, input) -> Self: ...
    @classmethod
    def from_wkb(cls, input: ArrowArrayExportable) -> Self: ...
    @classmethod
    def from_wkt(cls, input: ArrowArrayExportable) -> Self: ...
    def to_shapely(self) -> NDArray[np.object_]: ...
    def to_wkb(self) -> WKBArray: ...

class GeometryCollectionArray:
    def __eq__(self, other: Self) -> bool: ...
    @property
    def __geo_interface__(self) -> dict: ...
    def __getitem__(self, key: int) -> GeometryCollection: ...
    def __len__(self) -> int: ...
    def __arrow_c_array__(
        self, requested_schema: object | None = None
    ) -> Tuple[object, object]: ...
    def __repr__(self) -> str: ...
    def total_bounds(self) -> Tuple[float, float, float, float]: ...
    @classmethod
    def from_arrow(cls, input: ArrowArrayExportable) -> Self: ...
    @classmethod
    def from_ewkb(cls, input: ArrowArrayExportable) -> Self: ...
    @classmethod
    def from_shapely(cls, input) -> Self: ...
    @classmethod
    def from_wkb(cls, input: ArrowArrayExportable) -> Self: ...
    @classmethod
    def from_wkt(cls, input: ArrowArrayExportable) -> Self: ...
    def to_shapely(self) -> NDArray[np.object_]: ...
    def to_wkb(self) -> WKBArray: ...

class WKBArray:
    def __array__(self) -> NDArray[np.object_]: ...
    def __eq__(self, other: Self) -> bool: ...
    def __getitem__(self, key: int) -> WKB: ...
    def __len__(self) -> int: ...
    def __arrow_c_array__(
        self, requested_schema: object | None = None
    ) -> Tuple[object, object]: ...
    def __repr__(self) -> str: ...
    def total_bounds(self) -> Tuple[float, float, float, float]: ...
    @classmethod
    def from_arrow(cls, input: ArrowArrayExportable) -> Self: ...
    @classmethod
    def from_shapely(cls, input) -> Self: ...
    def to_shapely(self) -> NDArray[np.object_]: ...

class RectArray:
    def __eq__(self, other: Self) -> bool: ...
    def __getitem__(self, key: int) -> Rect: ...
    def __len__(self) -> int: ...
    def __repr__(self) -> str: ...
    def to_polygon_array(self) -> PolygonArray: ...
    def total_bounds(self) -> Tuple[float, float, float, float]: ...

class ChunkedPointArray:
    def __arrow_c_stream__(self, requested_schema: object | None = None) -> object: ...
    def __eq__(self, other: Self) -> bool: ...
    def __getitem__(self, key: int) -> Point: ...
    def __len__(self) -> int: ...
    def __repr__(self) -> str: ...
    def chunk(self, i: int) -> PointArray: ...
    def chunks(self) -> List[PointArray]: ...
    def concatenate(self) -> PointArray: ...
    @classmethod
    def from_arrow_arrays(cls, input: Sequence[ArrowArrayExportable]) -> Self: ...
    @classmethod
    def from_shapely(cls, input, *, chunk_size: int = 65536) -> Self: ...
    def num_chunks(self) -> int: ...
    def to_shapely(self) -> NDArray[np.object_]: ...
    def total_bounds(self) -> Tuple[float, float, float, float]: ...

class ChunkedLineStringArray:
    def __arrow_c_stream__(self, requested_schema: object | None = None) -> object: ...
    def __eq__(self, other: Self) -> bool: ...
    def __getitem__(self, key: int) -> LineString: ...
    def __len__(self) -> int: ...
    def __repr__(self) -> str: ...
    def chunk(self, i: int) -> LineStringArray: ...
    def chunks(self) -> List[LineStringArray]: ...
    def concatenate(self) -> LineStringArray: ...
    @classmethod
    def from_arrow_arrays(cls, input: Sequence[ArrowArrayExportable]) -> Self: ...
    @classmethod
    def from_shapely(cls, input, *, chunk_size: int = 65536) -> Self: ...
    def num_chunks(self) -> int: ...
    def to_shapely(self) -> NDArray[np.object_]: ...
    def total_bounds(self) -> Tuple[float, float, float, float]: ...

class ChunkedPolygonArray:
    def __arrow_c_stream__(self, requested_schema: object | None = None) -> object: ...
    def __eq__(self, other: Self) -> bool: ...
    def __getitem__(self, key: int) -> Polygon: ...
    def __len__(self) -> int: ...
    def __repr__(self) -> str: ...
    def chunk(self, i: int) -> PolygonArray: ...
    def chunks(self) -> List[PolygonArray]: ...
    def concatenate(self) -> PolygonArray: ...
    @classmethod
    def from_arrow_arrays(cls, input: Sequence[ArrowArrayExportable]) -> Self: ...
    @classmethod
    def from_shapely(cls, input, *, chunk_size: int = 65536) -> Self: ...
    def num_chunks(self) -> int: ...
    def polylabel(self, tolerance: float) -> ChunkedPointArray: ...
    def to_shapely(self) -> NDArray[np.object_]: ...
    def total_bounds(self) -> Tuple[float, float, float, float]: ...

class ChunkedMultiPointArray:
    def __arrow_c_stream__(self, requested_schema: object | None = None) -> object: ...
    def __eq__(self, other: Self) -> bool: ...
    def __getitem__(self, key: int) -> MultiPoint: ...
    def __len__(self) -> int: ...
    def __repr__(self) -> str: ...
    def chunk(self, i: int) -> MultiPointArray: ...
    def chunks(self) -> List[MultiPointArray]: ...
    def concatenate(self) -> MultiPointArray: ...
    @classmethod
    def from_arrow_arrays(cls, input: Sequence[ArrowArrayExportable]) -> Self: ...
    @classmethod
    def from_shapely(cls, input, *, chunk_size: int = 65536) -> Self: ...
    def num_chunks(self) -> int: ...
    def to_shapely(self) -> NDArray[np.object_]: ...
    def total_bounds(self) -> Tuple[float, float, float, float]: ...

class ChunkedMultiLineStringArray:
    def __arrow_c_stream__(self, requested_schema: object | None = None) -> object: ...
    def __eq__(self, other: Self) -> bool: ...
    def __getitem__(self, key: int) -> MultiLineString: ...
    def __len__(self) -> int: ...
    def __repr__(self) -> str: ...
    def chunk(self, i: int) -> MultiLineStringArray: ...
    def chunks(self) -> List[MultiLineStringArray]: ...
    def concatenate(self) -> MultiLineStringArray: ...
    @classmethod
    def from_arrow_arrays(cls, input: Sequence[ArrowArrayExportable]) -> Self: ...
    @classmethod
    def from_shapely(cls, input, *, chunk_size: int = 65536) -> Self: ...
    def num_chunks(self) -> int: ...
    def to_shapely(self) -> NDArray[np.object_]: ...
    def total_bounds(self) -> Tuple[float, float, float, float]: ...

class ChunkedMultiPolygonArray:
    def __arrow_c_stream__(self, requested_schema: object | None = None) -> object: ...
    def __eq__(self, other: Self) -> bool: ...
    def __getitem__(self, key: int) -> MultiPolygon: ...
    def __len__(self) -> int: ...
    def __repr__(self) -> str: ...
    def chunk(self, i: int) -> MultiPolygonArray: ...
    def chunks(self) -> List[MultiPolygonArray]: ...
    def concatenate(self) -> MultiPolygonArray: ...
    @classmethod
    def from_arrow_arrays(cls, input: Sequence[ArrowArrayExportable]) -> Self: ...
    @classmethod
    def from_shapely(cls, input, *, chunk_size: int = 65536) -> Self: ...
    def num_chunks(self) -> int: ...
    def to_shapely(self) -> NDArray[np.object_]: ...
    def total_bounds(self) -> Tuple[float, float, float, float]: ...

class ChunkedMixedGeometryArray:
    def __arrow_c_stream__(self, requested_schema: object | None = None) -> object: ...
    def __eq__(self, other: Self) -> bool: ...
    def __getitem__(self, key: int) -> Geometry: ...
    def __len__(self) -> int: ...
    def __repr__(self) -> str: ...
    def chunk(self, i: int) -> MixedGeometryArray: ...
    def chunks(self) -> List[MixedGeometryArray]: ...
    def concatenate(self) -> MixedGeometryArray: ...
    @classmethod
    def from_arrow_arrays(cls, input: Sequence[ArrowArrayExportable]) -> Self: ...
    @classmethod
    def from_shapely(cls, input, *, chunk_size: int = 65536) -> Self: ...
    def num_chunks(self) -> int: ...
    def to_shapely(self) -> NDArray[np.object_]: ...
    def total_bounds(self) -> Tuple[float, float, float, float]: ...

class ChunkedGeometryCollectionArray:
    def __arrow_c_stream__(self, requested_schema: object | None = None) -> object: ...
    def __eq__(self, other: Self) -> bool: ...
    def __getitem__(self, key: int) -> GeometryCollection: ...
    def __len__(self) -> int: ...
    def __repr__(self) -> str: ...
    def chunk(self, i: int) -> GeometryCollectionArray: ...
    def chunks(self) -> List[GeometryCollectionArray]: ...
    def concatenate(self) -> GeometryCollectionArray: ...
    @classmethod
    def from_arrow_arrays(cls, input: Sequence[ArrowArrayExportable]) -> Self: ...
    @classmethod
    def from_shapely(cls, input, *, chunk_size: int = 65536) -> Self: ...
    def num_chunks(self) -> int: ...
    def to_shapely(self) -> NDArray[np.object_]: ...
    def total_bounds(self) -> Tuple[float, float, float, float]: ...

class ChunkedWKBArray:
    def __arrow_c_stream__(self, requested_schema: object | None = None) -> object: ...
    def __array__(self) -> NDArray[np.object_]: ...
    def __eq__(self, other: Self) -> bool: ...
    def __getitem__(self, key: int) -> WKB: ...
    def __len__(self) -> int: ...
    def __repr__(self) -> str: ...
    def chunk(self, i: int) -> WKBArray: ...
    def chunks(self) -> List[WKBArray]: ...
    @classmethod
    def from_arrow_arrays(cls, input: Sequence[ArrowArrayExportable]) -> Self: ...
    @classmethod
    def from_shapely(cls, input, *, chunk_size: int = 65536) -> Self: ...
    def num_chunks(self) -> int: ...
    def to_shapely(self) -> NDArray[np.object_]: ...
    def total_bounds(self) -> Tuple[float, float, float, float]: ...

class ChunkedRectArray:
    def __arrow_c_stream__(self, requested_schema: object | None = None) -> object: ...
    def __eq__(self, other: Self) -> bool: ...
    def __getitem__(self, key: int) -> Rect: ...
    def __len__(self) -> int: ...
    def __repr__(self) -> str: ...
    def chunk(self, i: int) -> RectArray: ...
    def chunks(self) -> List[RectArray]: ...
    @classmethod
    def from_arrow_arrays(cls, input: Sequence[ArrowArrayExportable]) -> Self: ...
    def num_chunks(self) -> int: ...
    def total_bounds(self) -> Tuple[float, float, float, float]: ...

# Top-level array/chunked array functions

@overload
def affine_transform(
    input: AffineInputT,
    transform: AffineTransform,
) -> AffineInputT: ...
@overload
def affine_transform(
    input: ArrowArrayExportable,
    transform: AffineTransform,
) -> NativeGeometryArrayT: ...
@overload
def affine_transform(
    input: ArrowStreamExportable,
    transform: AffineTransform,
) -> NativeChunkedGeometryArrayT: ...
def affine_transform(
    input: AffineInputT | ArrowArrayExportable | ArrowStreamExportable,
    transform: AffineTransform,
) -> AffineInputT | NativeGeometryArrayT | NativeChunkedGeometryArrayT: ...
@overload
def area(
    input: ArrowArrayExportable,
    *,
    method: AreaMethod | AreaMethodT = AreaMethod.Euclidean,
) -> Array: ...
@overload
def area(
    input: ArrowStreamExportable,
    *,
    method: AreaMethod | AreaMethodT = AreaMethod.Euclidean,
) -> ChunkedArray: ...
def area(
    input: ArrowArrayExportable | ArrowStreamExportable,
    *,
    method: AreaMethod | AreaMethodT = AreaMethod.Euclidean,
) -> Array | ChunkedArray: ...
@overload
def center(input: ArrowArrayExportable) -> PointArray: ...
@overload
def center(input: ArrowStreamExportable) -> ChunkedPointArray: ...
def center(
    input: ArrowArrayExportable | ArrowStreamExportable,
) -> PointArray | ChunkedPointArray: ...
@overload
def centroid(input: ArrowArrayExportable) -> PointArray: ...
@overload
def centroid(input: ArrowStreamExportable) -> ChunkedPointArray: ...
def centroid(
    input: ArrowArrayExportable | ArrowStreamExportable,
) -> PointArray | ChunkedPointArray: ...

ChaikinSmoothingT = TypeVar(
    "ChaikinSmoothingT",
    LineStringArray,
    PolygonArray,
    MultiLineStringArray,
    MultiPolygonArray,
    ChunkedLineStringArray,
    ChunkedPolygonArray,
    ChunkedMultiLineStringArray,
    ChunkedMultiPolygonArray,
)

@overload
def chaikin_smoothing(
    input: ChaikinSmoothingT, n_iterations: int
) -> ChaikinSmoothingT: ...
@overload
def chaikin_smoothing(
    input: ArrowArrayExportable, n_iterations: int
) -> LineStringArray | PolygonArray | MultiLineStringArray | MultiPolygonArray: ...
@overload
def chaikin_smoothing(
    input: ArrowStreamExportable, n_iterations: int
) -> (
    ChunkedLineStringArray
    | ChunkedPolygonArray
    | ChunkedMultiLineStringArray
    | ChunkedMultiPolygonArray
): ...
def chaikin_smoothing(
    input: ChaikinSmoothingT | ArrowArrayExportable | ArrowStreamExportable,
    n_iterations: int,
) -> (
    LineStringArray
    | PolygonArray
    | MultiLineStringArray
    | MultiPolygonArray
    | ChunkedLineStringArray
    | ChunkedPolygonArray
    | ChunkedMultiLineStringArray
    | ChunkedMultiPolygonArray
): ...
@overload
def convex_hull(input: ArrowArrayExportable) -> PolygonArray: ...
@overload
def convex_hull(input: ArrowStreamExportable) -> ChunkedPolygonArray: ...
def convex_hull(
    input: ArrowArrayExportable | ArrowStreamExportable,
) -> PolygonArray | ChunkedPolygonArray: ...
def densify(
    input: ArrowArrayExportable, max_distance: float
) -> LineStringArray | PolygonArray | MultiLineStringArray | MultiPolygonArray: ...
@overload
def envelope(input: ArrowArrayExportable) -> RectArray: ...
@overload
def envelope(input: ArrowStreamExportable) -> ChunkedRectArray: ...
def envelope(
    input: ArrowArrayExportable | ArrowStreamExportable,
) -> RectArray | ChunkedRectArray: ...
@overload
def frechet_distance(
    input: ArrowArrayExportable,
    other: BroadcastGeometry,
) -> Array: ...
@overload
def frechet_distance(
    input: ArrowStreamExportable,
    other: BroadcastGeometry,
) -> ChunkedArray: ...
def frechet_distance(
    input: ArrowArrayExportable | ArrowStreamExportable,
    other: BroadcastGeometry,
) -> Array | ChunkedArray: ...
@overload
def geodesic_perimeter(
    input: ArrowArrayExportable,
) -> Array: ...
@overload
def geodesic_perimeter(
    input: ArrowStreamExportable,
) -> ChunkedArray: ...
def geodesic_perimeter(
    input: ArrowArrayExportable | ArrowStreamExportable,
) -> Array | ChunkedArray: ...
@overload
def is_empty(input: ArrowArrayExportable) -> Array: ...
@overload
def is_empty(input: ArrowStreamExportable) -> ChunkedArray: ...
def is_empty(
    input: ArrowArrayExportable | ArrowStreamExportable,
) -> Array | ChunkedArray: ...
@overload
def length(
    input: ArrowArrayExportable,
    *,
    method: LengthMethod | LengthMethodT = LengthMethod.Euclidean,
) -> Array: ...
@overload
def length(
    input: ArrowStreamExportable,
    *,
    method: LengthMethod | LengthMethodT = LengthMethod.Euclidean,
) -> ChunkedArray: ...
def length(
    input: ArrowArrayExportable | ArrowStreamExportable,
    *,
    method: LengthMethod | LengthMethodT = LengthMethod.Euclidean,
) -> Array | ChunkedArray: ...
@overload
def line_interpolate_point(
    input: ArrowArrayExportable,
    fraction: float | int | ArrowArrayExportable | NumpyArrayProtocolf64,
) -> PointArray: ...
@overload
def line_interpolate_point(
    input: ArrowStreamExportable,
    fraction: float | int | ArrowStreamExportable,
) -> ChunkedPointArray: ...
def line_interpolate_point(
    input: ArrowArrayExportable | ArrowStreamExportable,
    fraction: float
    | int
    | ArrowArrayExportable
    | ArrowStreamExportable
    | NumpyArrayProtocolf64,
) -> PointArray | ChunkedPointArray: ...
@overload
def line_locate_point(
    input: ArrowArrayExportable, point: GeoInterfaceProtocol | ArrowArrayExportable
) -> Array: ...
@overload
def line_locate_point(
    input: ArrowStreamExportable, point: GeoInterfaceProtocol | ArrowStreamExportable
) -> ChunkedArray: ...
def line_locate_point(
    input: ArrowArrayExportable | ArrowStreamExportable,
    point: GeoInterfaceProtocol | ArrowArrayExportable | ArrowStreamExportable,
) -> Array | ChunkedArray: ...
@overload
def polylabel(
    input: ArrowArrayExportable,
    tolerance: float,
) -> PointArray: ...
@overload
def polylabel(
    input: ArrowStreamExportable,
    tolerance: float,
) -> ChunkedPointArray: ...
def polylabel(
    input: ArrowArrayExportable | ArrowStreamExportable,
    tolerance: float,
) -> PointArray | ChunkedPointArray: ...
@overload
def signed_area(
    input: ArrowArrayExportable,
    *,
    method: AreaMethod | AreaMethodT = AreaMethod.Euclidean,
) -> Array: ...
@overload
def signed_area(
    input: ArrowStreamExportable,
    *,
    method: AreaMethod | AreaMethodT = AreaMethod.Euclidean,
) -> ChunkedArray: ...
def signed_area(
    input: ArrowArrayExportable | ArrowStreamExportable,
    *,
    method: AreaMethod | AreaMethodT = AreaMethod.Euclidean,
) -> Array | ChunkedArray: ...
@overload
def simplify(
    input: SimplifyInputT,
    epsilon: float,
    *,
    method: SimplifyMethod | SimplifyMethodT = SimplifyMethod.RDP,
) -> SimplifyInputT: ...
@overload
def simplify(
    input: ArrowArrayExportable,
    epsilon: float,
    *,
    method: SimplifyMethod | SimplifyMethodT = SimplifyMethod.RDP,
) -> (
    PointArray
    | LineStringArray
    | PolygonArray
    | MultiPointArray
    | MultiLineStringArray
    | MultiPolygonArray
): ...
@overload
def simplify(
    input: ArrowStreamExportable,
    epsilon: float,
    *,
    method: SimplifyMethod | SimplifyMethodT = SimplifyMethod.RDP,
) -> (
    ChunkedPointArray
    | ChunkedLineStringArray
    | ChunkedPolygonArray
    | ChunkedMultiPointArray
    | ChunkedMultiLineStringArray
    | ChunkedMultiPolygonArray
): ...
def simplify(
    input: SimplifyInputT | ArrowArrayExportable | ArrowStreamExportable,
    epsilon: float,
    *,
    method: SimplifyMethod | SimplifyMethodT = SimplifyMethod.RDP,
) -> (
    SimplifyInputT
    | PointArray
    | LineStringArray
    | PolygonArray
    | MultiPointArray
    | MultiLineStringArray
    | MultiPolygonArray
    | ChunkedPointArray
    | ChunkedLineStringArray
    | ChunkedPolygonArray
    | ChunkedMultiPointArray
    | ChunkedMultiLineStringArray
    | ChunkedMultiPolygonArray
): ...
def total_bounds(
    input: ArrowArrayExportable | ArrowStreamExportable,
) -> Tuple[float, float, float, float]: ...

# Top-level table functions

def explode(input: ArrowStreamExportable) -> Table: ...
def geometry_col(
    table: ArrowStreamExportable,
) -> (
    ChunkedPointArray
    | ChunkedLineStringArray
    | ChunkedPolygonArray
    | ChunkedMultiPointArray
    | ChunkedMultiLineStringArray
    | ChunkedMultiPolygonArray
    | ChunkedMixedGeometryArray
    | ChunkedGeometryCollectionArray
): ...

# I/O

class ParquetFile:
    def __init__(self, path: str, fs: ObjectStore) -> None: ...
    @property
    def num_rows(self) -> int: ...
    @property
    def num_row_groups(self) -> int: ...
    def row_group_bounds(
        self,
        minx_path: Sequence[str],
        miny_path: Sequence[str],
        maxx_path: Sequence[str],
        maxy_path: Sequence[str],
        row_group_idx: int,
    ) -> Tuple[float, float, float, float]: ...
    def row_groups_bounds(
        self,
        minx_path: Sequence[str],
        miny_path: Sequence[str],
        maxx_path: Sequence[str],
        maxy_path: Sequence[str],
    ) -> PolygonArray: ...
    def file_bbox(self) -> List[float] | None: ...
    async def read_async(
        self,
        *,
        batch_size: int | None = None,
        limit: int | None = None,
        offset: int | None = None,
        bbox: Sequence[IntFloat] | None = None,
        bbox_paths: BboxPaths | None = None,
    ) -> Table: ...
    def read(
        self,
        *,
        batch_size: int | None = None,
        limit: int | None = None,
        offset: int | None = None,
        bbox: Sequence[IntFloat] | None = None,
        bbox_paths: BboxPaths | None = None,
    ) -> Table: ...
    async def read_row_groups_async(self, row_groups: Sequence[int]) -> Table: ...
    def read_row_groups(self, row_groups: Sequence[int]) -> Table: ...

class ParquetDataset:
    def __init__(self, paths: Sequence[str], fs: ObjectStore) -> None: ...
    @property
    def num_rows(self) -> int: ...
    @property
    def num_row_groups(self) -> int: ...
    async def read_async(
        self,
        *,
        batch_size: int | None = None,
        limit: int | None = None,
        offset: int | None = None,
        bbox: Sequence[IntFloat] | None = None,
        bbox_paths: BboxPaths | None = None,
    ) -> Table: ...
    def read(
        self,
        *,
        batch_size: int | None = None,
        limit: int | None = None,
        offset: int | None = None,
        bbox: Sequence[IntFloat] | None = None,
        bbox_paths: BboxPaths | None = None,
    ) -> Table: ...

class ParquetWriter:
    def __init__(
        self, file: str | Path | BinaryIO, schema: ArrowSchemaExportable
    ) -> None: ...
    def __enter__(self): ...
    def __exit__(self, type, value, traceback): ...
    def close(self) -> None: ...
    def is_closed(self) -> bool: ...
    def write_batch(self, batch: ArrowArrayExportable) -> None: ...
    def write_table(self, table: ArrowStreamExportable) -> None: ...

class ObjectStore:
    def __init__(self, root: str, options: Optional[Dict[str, str]] = None) -> None: ...

def read_csv(
    file: str | Path | BinaryIO,
    geometry_column_name: str,
    *,
    batch_size: int = 65536,
) -> Table: ...
def read_flatgeobuf(
    file: Union[str, Path, BinaryIO],
    *,
    fs: Optional[ObjectStore] = None,
    batch_size: int = 65536,
    bbox: Tuple[float, float, float, float] | None = None,
) -> Table: ...
async def read_flatgeobuf_async(
    path: str,
    *,
    fs: Optional[ObjectStore] = None,
    batch_size: int = 65536,
    bbox: Tuple[float, float, float, float] | None = None,
) -> Table: ...
def read_geojson(
    file: Union[str, Path, BinaryIO], *, batch_size: int = 65536
) -> Table: ...
def read_geojson_lines(
    file: Union[str, Path, BinaryIO], *, batch_size: int = 65536
) -> Table: ...
def read_ipc(file: Union[str, Path, BinaryIO]) -> Table: ...
def read_ipc_stream(file: Union[str, Path, BinaryIO]) -> Table: ...
def read_parquet(
    path: str, *, fs: Optional[ObjectStore] = None, batch_size: int = 65536
) -> Table: ...
async def read_parquet_async(
    path: str, *, fs: Optional[ObjectStore] = None, batch_size: int = 65536
) -> Table: ...
def read_postgis(connection_url: str, sql: str) -> Optional[Table]: ...
async def read_postgis_async(connection_url: str, sql: str) -> Optional[Table]: ...
def read_pyogrio(
    path_or_buffer: Path | str | bytes,
    /,
    layer: int | str | None = None,
    encoding: str | None = None,
    columns: Sequence[str] | None = None,
    read_geometry: bool = True,
    skip_features: int = 0,
    max_features: int | None = None,
    where: str | None = None,
    bbox: Tuple[float, float, float, float] | Sequence[float] | None = None,
    mask=None,
    fids=None,
    sql: str | None = None,
    sql_dialect: str | None = None,
    return_fids=False,
    batch_size=65536,
    **kwargs,
) -> RecordBatchReader: ...
def write_csv(table: ArrowStreamExportable, file: str | Path | BinaryIO) -> None: ...
def write_flatgeobuf(
    table: ArrowStreamExportable,
    file: str | Path | BinaryIO,
    *,
    write_index: bool = True,
) -> None: ...
def write_geojson(
    table: ArrowStreamExportable, file: Union[str, Path, BinaryIO]
) -> None: ...
def write_geojson_lines(
    table: ArrowStreamExportable, file: Union[str, Path, BinaryIO]
) -> None: ...
def write_ipc(
    table: ArrowStreamExportable, file: Union[str, Path, BinaryIO]
) -> None: ...
def write_ipc_stream(
    table: ArrowStreamExportable, file: Union[str, Path, BinaryIO]
) -> None: ...
def write_parquet(
    table: ArrowStreamExportable,
    file: str,
    *,
    encoding: GeoParquetEncoding | GeoParquetEncodingT = GeoParquetEncoding.WKB,
) -> None: ...

# Interop
def from_ewkb(
    input: ArrowArrayExportable,
) -> (
    PointArray
    | LineStringArray
    | PolygonArray
    | MultiPointArray
    | MultiLineStringArray
    | MultiPolygonArray
    | MixedGeometryArray
    | GeometryCollectionArray
): ...
def from_geopandas(input: gpd.GeoDataFrame) -> Table: ...
def from_shapely(
    input,
) -> (
    PointArray
    | LineStringArray
    | PolygonArray
    | MultiPointArray
    | MultiLineStringArray
    | MultiPolygonArray
    | MixedGeometryArray
): ...
def from_wkb(
    input: ArrowArrayExportable,
) -> (
    PointArray
    | LineStringArray
    | PolygonArray
    | MultiPointArray
    | MultiLineStringArray
    | MultiPolygonArray
    | MixedGeometryArray
    | GeometryCollectionArray
): ...
def from_wkt(
    input: ArrowArrayExportable,
) -> (
    PointArray
    | LineStringArray
    | PolygonArray
    | MultiPointArray
    | MultiLineStringArray
    | MultiPolygonArray
    | MixedGeometryArray
    | GeometryCollectionArray
): ...
def to_geopandas(input: ArrowStreamExportable) -> gpd.GeoDataFrame: ...
def to_shapely(input: ArrowArrayExportable) -> NDArray[np.object_]: ...
def to_wkb(input: ArrowArrayExportable) -> WKBArray: ...
