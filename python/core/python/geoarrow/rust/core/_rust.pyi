from __future__ import annotations

from pathlib import Path
from typing import BinaryIO, List, Optional, Self, Sequence, Tuple, Union

try:
    import numpy as np
    from numpy.typing import NDArray
except ImportError:
    pass

try:
    import geopandas as gpd
except ImportError:
    pass

from .types import ArrowArrayExportable, ArrowStreamExportable

class Point:
    def __eq__(self, other: Self) -> bool: ...
    @property
    def __geo_interface__(self) -> dict: ...
    def _repr_svg_(self) -> str: ...

class LineString:
    def __eq__(self, other: Self) -> bool: ...
    @property
    def __geo_interface__(self) -> dict: ...
    def _repr_svg_(self) -> str: ...

class Polygon:
    def __eq__(self, other: Self) -> bool: ...
    @property
    def __geo_interface__(self) -> dict: ...
    def _repr_svg_(self) -> str: ...

class MultiPoint:
    def __eq__(self, other: Self) -> bool: ...
    @property
    def __geo_interface__(self) -> dict: ...
    def _repr_svg_(self) -> str: ...

class MultiLineString:
    def __eq__(self, other: Self) -> bool: ...
    @property
    def __geo_interface__(self) -> dict: ...
    def _repr_svg_(self) -> str: ...

class MultiPolygon:
    def __eq__(self, other: Self) -> bool: ...
    @property
    def __geo_interface__(self) -> dict: ...
    def _repr_svg_(self) -> str: ...

class Geometry:
    def __eq__(self, other: Self) -> bool: ...
    @property
    def __geo_interface__(self) -> dict: ...
    def _repr_svg_(self) -> str: ...

class GeometryCollection:
    def __eq__(self, other: Self) -> bool: ...
    @property
    def __geo_interface__(self) -> dict: ...
    def _repr_svg_(self) -> str: ...

class WKB:
    def __eq__(self, other: Self) -> bool: ...

class Rect:
    def __eq__(self, other: Self) -> bool: ...

class PointArray:
    def __eq__(self, other: Self) -> bool: ...
    @property
    def __geo_interface__(self) -> dict: ...
    def __getitem__(self, key: int) -> Point: ...
    def __len__(self) -> int: ...
    def __arrow_c_array__(
        self, requested_schema: object | None = None
    ) -> Tuple[object, object]: ...
    def area(self) -> Float64Array: ...
    def bounding_rect(self) -> RectArray: ...
    def center(self) -> PointArray: ...
    def centroid(self) -> PointArray: ...
    def chamberlain_duquette_signed_area(self) -> Float64Array: ...
    def chamberlain_duquette_unsigned_area(self) -> Float64Array: ...
    def convex_hull(self) -> PolygonArray: ...
    def geodesic_area_signed(self) -> Float64Array: ...
    def geodesic_area_unsigned(self) -> Float64Array: ...
    def geodesic_length(self) -> Float64Array: ...
    def geodesic_perimeter(self) -> Float64Array: ...
    def is_empty(self) -> BooleanArray: ...
    def length(self) -> Float64Array: ...
    def simplify(self, epsilon: float) -> Self: ...
    def simplify_vw(self, epsilon: float) -> Self: ...
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
    def area(self) -> Float64Array: ...
    def bounding_rect(self) -> RectArray: ...
    def center(self) -> PointArray: ...
    def centroid(self) -> PointArray: ...
    def chaikin_smoothing(self, n_iterations: int) -> Self: ...
    def chamberlain_duquette_signed_area(self) -> Float64Array: ...
    def chamberlain_duquette_unsigned_area(self) -> Float64Array: ...
    def convex_hull(self) -> PolygonArray: ...
    def densify(self, max_distance: float) -> Self: ...
    def geodesic_area_signed(self) -> Float64Array: ...
    def geodesic_area_unsigned(self) -> Float64Array: ...
    def geodesic_length(self) -> Float64Array: ...
    def geodesic_perimeter(self) -> Float64Array: ...
    def is_empty(self) -> BooleanArray: ...
    def length(self) -> Float64Array: ...
    def simplify(self, epsilon: float) -> Self: ...
    def simplify_vw(self, epsilon: float) -> Self: ...
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
    def area(self) -> Float64Array: ...
    def bounding_rect(self) -> RectArray: ...
    def center(self) -> PointArray: ...
    def centroid(self) -> PointArray: ...
    def chaikin_smoothing(self, n_iterations: int) -> Self: ...
    def chamberlain_duquette_signed_area(self) -> Float64Array: ...
    def chamberlain_duquette_unsigned_area(self) -> Float64Array: ...
    def convex_hull(self) -> PolygonArray: ...
    def densify(self, max_distance: float) -> Self: ...
    def geodesic_area_signed(self) -> Float64Array: ...
    def geodesic_area_unsigned(self) -> Float64Array: ...
    def geodesic_perimeter(self) -> Float64Array: ...
    def is_empty(self) -> BooleanArray: ...
    def simplify(self, epsilon: float) -> Self: ...
    def simplify_vw(self, epsilon: float) -> Self: ...
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
    def area(self) -> Float64Array: ...
    def bounding_rect(self) -> RectArray: ...
    def center(self) -> PointArray: ...
    def centroid(self) -> PointArray: ...
    def chamberlain_duquette_signed_area(self) -> Float64Array: ...
    def chamberlain_duquette_unsigned_area(self) -> Float64Array: ...
    def convex_hull(self) -> PolygonArray: ...
    def geodesic_area_signed(self) -> Float64Array: ...
    def geodesic_area_unsigned(self) -> Float64Array: ...
    def geodesic_length(self) -> Float64Array: ...
    def geodesic_perimeter(self) -> Float64Array: ...
    def is_empty(self) -> BooleanArray: ...
    def length(self) -> Float64Array: ...
    def simplify(self, epsilon: float) -> Self: ...
    def simplify_vw(self, epsilon: float) -> Self: ...
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
    def area(self) -> Float64Array: ...
    def bounding_rect(self) -> RectArray: ...
    def center(self) -> PointArray: ...
    def centroid(self) -> PointArray: ...
    def chaikin_smoothing(self, n_iterations: int) -> Self: ...
    def chamberlain_duquette_signed_area(self) -> Float64Array: ...
    def chamberlain_duquette_unsigned_area(self) -> Float64Array: ...
    def convex_hull(self) -> PolygonArray: ...
    def densify(self, max_distance: float) -> Self: ...
    def geodesic_area_signed(self) -> Float64Array: ...
    def geodesic_area_unsigned(self) -> Float64Array: ...
    def geodesic_length(self) -> Float64Array: ...
    def geodesic_perimeter(self) -> Float64Array: ...
    def is_empty(self) -> BooleanArray: ...
    def length(self) -> Float64Array: ...
    def simplify(self, epsilon: float) -> Self: ...
    def simplify_vw(self, epsilon: float) -> Self: ...
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
    def area(self) -> Float64Array: ...
    def bounding_rect(self) -> RectArray: ...
    def center(self) -> PointArray: ...
    def centroid(self) -> PointArray: ...
    def chaikin_smoothing(self, n_iterations: int) -> Self: ...
    def chamberlain_duquette_signed_area(self) -> Float64Array: ...
    def chamberlain_duquette_unsigned_area(self) -> Float64Array: ...
    def convex_hull(self) -> PolygonArray: ...
    def densify(self, max_distance: float) -> Self: ...
    def geodesic_area_signed(self) -> Float64Array: ...
    def geodesic_area_unsigned(self) -> Float64Array: ...
    def geodesic_perimeter(self) -> Float64Array: ...
    def is_empty(self) -> BooleanArray: ...
    def simplify(self, epsilon: float) -> Self: ...
    def simplify_vw(self, epsilon: float) -> Self: ...
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
    def area(self) -> Float64Array: ...
    def bounding_rect(self) -> RectArray: ...
    def center(self) -> PointArray: ...
    def centroid(self) -> PointArray: ...
    def chamberlain_duquette_signed_area(self) -> Float64Array: ...
    def chamberlain_duquette_unsigned_area(self) -> Float64Array: ...
    def convex_hull(self) -> PolygonArray: ...
    def geodesic_area_signed(self) -> Float64Array: ...
    def geodesic_area_unsigned(self) -> Float64Array: ...
    def geodesic_perimeter(self) -> Float64Array: ...
    def is_empty(self) -> BooleanArray: ...
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
    def area(self) -> Float64Array: ...
    def bounding_rect(self) -> RectArray: ...
    def center(self) -> PointArray: ...
    def centroid(self) -> PointArray: ...
    def chamberlain_duquette_signed_area(self) -> Float64Array: ...
    def chamberlain_duquette_unsigned_area(self) -> Float64Array: ...
    def convex_hull(self) -> PolygonArray: ...
    def geodesic_area_signed(self) -> Float64Array: ...
    def geodesic_area_unsigned(self) -> Float64Array: ...
    def geodesic_perimeter(self) -> Float64Array: ...
    def is_empty(self) -> BooleanArray: ...
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
    @classmethod
    def from_arrow(cls, input: ArrowArrayExportable) -> Self: ...
    @classmethod
    def from_shapely(cls, input) -> Self: ...
    def to_shapely(self) -> NDArray[np.object_]: ...

class RectArray:
    def __eq__(self, other: Self) -> bool: ...
    def __getitem__(self, key: int) -> Rect: ...
    def __len__(self) -> int: ...
    def to_polygon_array(self) -> PolygonArray: ...

class BooleanArray:
    def __eq__(self, other: Self) -> bool: ...
    def __len__(self) -> int: ...
    def __arrow_c_array__(
        self, requested_schema: object | None = None
    ) -> Tuple[object, object]: ...

# class Float16Array:
#     def __eq__(self, other: Self) -> bool: ...
#     def __len__(self) -> int: ...
#     def __arrow_c_array__(
#         self, requested_schema: object | None = None
#     ) -> Tuple[object, object]: ...

# class Float32Array:
#     def __eq__(self, other: Self) -> bool: ...
#     def __len__(self) -> int: ...
#     def __arrow_c_array__(
#         self, requested_schema: object | None = None
#     ) -> Tuple[object, object]: ...
#     def to_numpy(self) -> NDArray[np.float32]: ...

class Float64Array:
    def __eq__(self, other: Self) -> bool: ...
    def __len__(self) -> int: ...
    def __arrow_c_array__(
        self, requested_schema: object | None = None
    ) -> Tuple[object, object]: ...
    def to_numpy(self) -> NDArray[np.float64]: ...

# class Int16Array:
#     def __eq__(self, other: Self) -> bool: ...
#     def __len__(self) -> int: ...
#     def __arrow_c_array__(
#         self, requested_schema: object | None = None
#     ) -> Tuple[object, object]: ...
#     def to_numpy(self) -> NDArray[np.int16]: ...

# class Int32Array:
#     def __eq__(self, other: Self) -> bool: ...
#     def __len__(self) -> int: ...
#     def __arrow_c_array__(
#         self, requested_schema: object | None = None
#     ) -> Tuple[object, object]: ...
#     def to_numpy(self) -> NDArray[np.int32]: ...

# class Int64Array:
#     def __eq__(self, other: Self) -> bool: ...
#     def __len__(self) -> int: ...
#     def __arrow_c_array__(
#         self, requested_schema: object | None = None
#     ) -> Tuple[object, object]: ...
#     def to_numpy(self) -> NDArray[np.int64]: ...

# class Int8Array:
#     def __eq__(self, other: Self) -> bool: ...
#     def __len__(self) -> int: ...
#     def __arrow_c_array__(
#         self, requested_schema: object | None = None
#     ) -> Tuple[object, object]: ...
#     def to_numpy(self) -> NDArray[np.int8]: ...

# class LargeStringArray:
#     def __eq__(self, other: Self) -> bool: ...
#     def __len__(self) -> int: ...
#     def __arrow_c_array__(
#         self, requested_schema: object | None = None
#     ) -> Tuple[object, object]: ...

# class StringArray:
#     def __eq__(self, other: Self) -> bool: ...
#     def __len__(self) -> int: ...
#     def __arrow_c_array__(
#         self, requested_schema: object | None = None
#     ) -> Tuple[object, object]: ...

# class UInt16Array:
#     def __eq__(self, other: Self) -> bool: ...
#     def __len__(self) -> int: ...
#     def __arrow_c_array__(
#         self, requested_schema: object | None = None
#     ) -> Tuple[object, object]: ...
#     def to_numpy(self) -> NDArray[np.uint16]: ...

# class UInt32Array:
#     def __eq__(self, other: Self) -> bool: ...
#     def __len__(self) -> int: ...
#     def __arrow_c_array__(
#         self, requested_schema: object | None = None
#     ) -> Tuple[object, object]: ...
#     def to_numpy(self) -> NDArray[np.uint32]: ...

# class UInt64Array:
#     def __eq__(self, other: Self) -> bool: ...
#     def __len__(self) -> int: ...
#     def __arrow_c_array__(
#         self, requested_schema: object | None = None
#     ) -> Tuple[object, object]: ...
#     def to_numpy(self) -> NDArray[np.uint64]: ...

# class UInt8Array:
#     def __eq__(self, other: Self) -> bool: ...
#     def __len__(self) -> int: ...
#     def __arrow_c_array__(
#         self, requested_schema: object | None = None
#     ) -> Tuple[object, object]: ...
#     def to_numpy(self) -> NDArray[np.uint8]: ...

class ChunkedPointArray:
    def __eq__(self, other: Self) -> bool: ...
    def __len__(self) -> int: ...
    def area(self) -> ChunkedFloat64Array: ...
    def bounding_rect(self) -> ChunkedRectArray: ...
    def center(self) -> ChunkedPointArray: ...
    def centroid(self) -> ChunkedPointArray: ...
    def chamberlain_duquette_signed_area(self) -> ChunkedFloat64Array: ...
    def chamberlain_duquette_unsigned_area(self) -> ChunkedFloat64Array: ...
    def chunk(self, i: int) -> PointArray: ...
    def chunks(self) -> List[PointArray]: ...
    def concatenate(self) -> PointArray: ...
    def convex_hull(self) -> ChunkedPolygonArray: ...
    @classmethod
    def from_arrow_arrays(cls, input: Sequence[ArrowArrayExportable]) -> Self: ...
    @classmethod
    def from_shapely(cls, input, *, chunk_size: int = 65536) -> Self: ...
    def geodesic_area_signed(self) -> ChunkedFloat64Array: ...
    def geodesic_area_unsigned(self) -> ChunkedFloat64Array: ...
    def geodesic_length(self) -> ChunkedFloat64Array: ...
    def geodesic_perimeter(self) -> ChunkedFloat64Array: ...
    def is_empty(self) -> BooleanArray: ...
    def length(self) -> ChunkedFloat64Array: ...
    def num_chunks(self) -> int: ...
    def simplify(self, epsilon: float) -> Self: ...
    def simplify_vw(self, epsilon: float) -> Self: ...
    def to_shapely(self) -> NDArray[np.object_]: ...

class ChunkedLineStringArray:
    def __eq__(self, other: Self) -> bool: ...
    def __len__(self) -> int: ...
    def area(self) -> ChunkedFloat64Array: ...
    def bounding_rect(self) -> ChunkedRectArray: ...
    def center(self) -> ChunkedPointArray: ...
    def centroid(self) -> ChunkedPointArray: ...
    def chaikin_smoothing(self, n_iterations: int) -> Self: ...
    def chamberlain_duquette_signed_area(self) -> ChunkedFloat64Array: ...
    def chamberlain_duquette_unsigned_area(self) -> ChunkedFloat64Array: ...
    def chunk(self, i: int) -> LineStringArray: ...
    def chunks(self) -> List[LineStringArray]: ...
    def concatenate(self) -> LineStringArray: ...
    def convex_hull(self) -> ChunkedPolygonArray: ...
    def densify(self, max_distance: float) -> Self: ...
    @classmethod
    def from_arrow_arrays(cls, input: Sequence[ArrowArrayExportable]) -> Self: ...
    @classmethod
    def from_shapely(cls, input, *, chunk_size: int = 65536) -> Self: ...
    def geodesic_area_signed(self) -> ChunkedFloat64Array: ...
    def geodesic_area_unsigned(self) -> ChunkedFloat64Array: ...
    def geodesic_length(self) -> ChunkedFloat64Array: ...
    def geodesic_perimeter(self) -> ChunkedFloat64Array: ...
    def is_empty(self) -> BooleanArray: ...
    def length(self) -> ChunkedFloat64Array: ...
    def num_chunks(self) -> int: ...
    def simplify(self, epsilon: float) -> Self: ...
    def simplify_vw(self, epsilon: float) -> Self: ...
    def to_shapely(self) -> NDArray[np.object_]: ...

class ChunkedPolygonArray:
    def __eq__(self, other: Self) -> bool: ...
    def __len__(self) -> int: ...
    def area(self) -> ChunkedFloat64Array: ...
    def bounding_rect(self) -> ChunkedRectArray: ...
    def center(self) -> ChunkedPointArray: ...
    def centroid(self) -> ChunkedPointArray: ...
    def chaikin_smoothing(self, n_iterations: int) -> Self: ...
    def chamberlain_duquette_signed_area(self) -> ChunkedFloat64Array: ...
    def chamberlain_duquette_unsigned_area(self) -> ChunkedFloat64Array: ...
    def chunk(self, i: int) -> PolygonArray: ...
    def chunks(self) -> List[PolygonArray]: ...
    def concatenate(self) -> PolygonArray: ...
    def convex_hull(self) -> ChunkedPolygonArray: ...
    def densify(self, max_distance: float) -> Self: ...
    @classmethod
    def from_arrow_arrays(cls, input: Sequence[ArrowArrayExportable]) -> Self: ...
    @classmethod
    def from_shapely(cls, input, *, chunk_size: int = 65536) -> Self: ...
    def geodesic_area_signed(self) -> ChunkedFloat64Array: ...
    def geodesic_area_unsigned(self) -> ChunkedFloat64Array: ...
    def geodesic_perimeter(self) -> ChunkedFloat64Array: ...
    def is_empty(self) -> BooleanArray: ...
    def num_chunks(self) -> int: ...
    def simplify(self, epsilon: float) -> Self: ...
    def simplify_vw(self, epsilon: float) -> Self: ...
    def to_shapely(self) -> NDArray[np.object_]: ...

class ChunkedMultiPointArray:
    def __eq__(self, other: Self) -> bool: ...
    def __len__(self) -> int: ...
    def area(self) -> ChunkedFloat64Array: ...
    def bounding_rect(self) -> ChunkedRectArray: ...
    def center(self) -> ChunkedPointArray: ...
    def centroid(self) -> ChunkedPointArray: ...
    def chamberlain_duquette_signed_area(self) -> ChunkedFloat64Array: ...
    def chamberlain_duquette_unsigned_area(self) -> ChunkedFloat64Array: ...
    def chunk(self, i: int) -> MultiPointArray: ...
    def chunks(self) -> List[MultiPointArray]: ...
    def concatenate(self) -> MultiPointArray: ...
    def convex_hull(self) -> ChunkedPolygonArray: ...
    @classmethod
    def from_arrow_arrays(cls, input: Sequence[ArrowArrayExportable]) -> Self: ...
    @classmethod
    def from_shapely(cls, input, *, chunk_size: int = 65536) -> Self: ...
    def geodesic_area_signed(self) -> ChunkedFloat64Array: ...
    def geodesic_area_unsigned(self) -> ChunkedFloat64Array: ...
    def geodesic_length(self) -> ChunkedFloat64Array: ...
    def geodesic_perimeter(self) -> ChunkedFloat64Array: ...
    def is_empty(self) -> BooleanArray: ...
    def length(self) -> ChunkedFloat64Array: ...
    def num_chunks(self) -> int: ...
    def simplify(self, epsilon: float) -> Self: ...
    def simplify_vw(self, epsilon: float) -> Self: ...
    def to_shapely(self) -> NDArray[np.object_]: ...

class ChunkedMultiLineStringArray:
    def __eq__(self, other: Self) -> bool: ...
    def __len__(self) -> int: ...
    def area(self) -> ChunkedFloat64Array: ...
    def bounding_rect(self) -> ChunkedRectArray: ...
    def center(self) -> ChunkedPointArray: ...
    def centroid(self) -> ChunkedPointArray: ...
    def chaikin_smoothing(self, n_iterations: int) -> Self: ...
    def chamberlain_duquette_signed_area(self) -> ChunkedFloat64Array: ...
    def chamberlain_duquette_unsigned_area(self) -> ChunkedFloat64Array: ...
    def chunk(self, i: int) -> MultiLineStringArray: ...
    def chunks(self) -> List[MultiLineStringArray]: ...
    def concatenate(self) -> MultiLineStringArray: ...
    def convex_hull(self) -> ChunkedPolygonArray: ...
    def densify(self, max_distance: float) -> Self: ...
    @classmethod
    def from_arrow_arrays(cls, input: Sequence[ArrowArrayExportable]) -> Self: ...
    @classmethod
    def from_shapely(cls, input, *, chunk_size: int = 65536) -> Self: ...
    def geodesic_area_signed(self) -> ChunkedFloat64Array: ...
    def geodesic_area_unsigned(self) -> ChunkedFloat64Array: ...
    def geodesic_length(self) -> ChunkedFloat64Array: ...
    def geodesic_perimeter(self) -> ChunkedFloat64Array: ...
    def is_empty(self) -> BooleanArray: ...
    def length(self) -> ChunkedFloat64Array: ...
    def num_chunks(self) -> int: ...
    def simplify(self, epsilon: float) -> Self: ...
    def simplify_vw(self, epsilon: float) -> Self: ...
    def to_shapely(self) -> NDArray[np.object_]: ...

class ChunkedMultiPolygonArray:
    def __eq__(self, other: Self) -> bool: ...
    def __len__(self) -> int: ...
    def area(self) -> ChunkedFloat64Array: ...
    def bounding_rect(self) -> ChunkedRectArray: ...
    def center(self) -> ChunkedPointArray: ...
    def centroid(self) -> ChunkedPointArray: ...
    def chaikin_smoothing(self, n_iterations: int) -> Self: ...
    def chamberlain_duquette_signed_area(self) -> ChunkedFloat64Array: ...
    def chamberlain_duquette_unsigned_area(self) -> ChunkedFloat64Array: ...
    def chunk(self, i: int) -> MultiPolygonArray: ...
    def chunks(self) -> List[MultiPolygonArray]: ...
    def concatenate(self) -> MultiPolygonArray: ...
    def convex_hull(self) -> ChunkedPolygonArray: ...
    def densify(self, max_distance: float) -> Self: ...
    @classmethod
    def from_arrow_arrays(cls, input: Sequence[ArrowArrayExportable]) -> Self: ...
    @classmethod
    def from_shapely(cls, input, *, chunk_size: int = 65536) -> Self: ...
    def geodesic_area_signed(self) -> ChunkedFloat64Array: ...
    def geodesic_area_unsigned(self) -> ChunkedFloat64Array: ...
    def geodesic_perimeter(self) -> ChunkedFloat64Array: ...
    def is_empty(self) -> BooleanArray: ...
    def num_chunks(self) -> int: ...
    def simplify(self, epsilon: float) -> Self: ...
    def simplify_vw(self, epsilon: float) -> Self: ...
    def to_shapely(self) -> NDArray[np.object_]: ...

class ChunkedMixedGeometryArray:
    def __eq__(self, other: Self) -> bool: ...
    def __len__(self) -> int: ...
    def area(self) -> ChunkedFloat64Array: ...
    def bounding_rect(self) -> ChunkedRectArray: ...
    def center(self) -> ChunkedPointArray: ...
    def centroid(self) -> ChunkedPointArray: ...
    def chamberlain_duquette_signed_area(self) -> ChunkedFloat64Array: ...
    def chamberlain_duquette_unsigned_area(self) -> ChunkedFloat64Array: ...
    def chunk(self, i: int) -> MixedGeometryArray: ...
    def chunks(self) -> List[MixedGeometryArray]: ...
    def concatenate(self) -> MixedGeometryArray: ...
    def convex_hull(self) -> ChunkedPolygonArray: ...
    @classmethod
    def from_arrow_arrays(cls, input: Sequence[ArrowArrayExportable]) -> Self: ...
    @classmethod
    def from_shapely(cls, input, *, chunk_size: int = 65536) -> Self: ...
    def geodesic_area_signed(self) -> ChunkedFloat64Array: ...
    def geodesic_area_unsigned(self) -> ChunkedFloat64Array: ...
    def geodesic_perimeter(self) -> ChunkedFloat64Array: ...
    def is_empty(self) -> BooleanArray: ...
    def num_chunks(self) -> int: ...
    def to_shapely(self) -> NDArray[np.object_]: ...

class ChunkedGeometryCollectionArray:
    def __eq__(self, other: Self) -> bool: ...
    def __len__(self) -> int: ...
    def area(self) -> ChunkedFloat64Array: ...
    def bounding_rect(self) -> ChunkedRectArray: ...
    def center(self) -> ChunkedPointArray: ...
    def centroid(self) -> ChunkedPointArray: ...
    def chamberlain_duquette_signed_area(self) -> ChunkedFloat64Array: ...
    def chamberlain_duquette_unsigned_area(self) -> ChunkedFloat64Array: ...
    def chunk(self, i: int) -> GeometryCollectionArray: ...
    def chunks(self) -> List[GeometryCollectionArray]: ...
    def concatenate(self) -> GeometryCollectionArray: ...
    def convex_hull(self) -> ChunkedPolygonArray: ...
    @classmethod
    def from_arrow_arrays(cls, input: Sequence[ArrowArrayExportable]) -> Self: ...
    @classmethod
    def from_shapely(cls, input, *, chunk_size: int = 65536) -> Self: ...
    def geodesic_area_signed(self) -> ChunkedFloat64Array: ...
    def geodesic_area_unsigned(self) -> ChunkedFloat64Array: ...
    def geodesic_perimeter(self) -> ChunkedFloat64Array: ...
    def is_empty(self) -> BooleanArray: ...
    def num_chunks(self) -> int: ...
    def to_shapely(self) -> NDArray[np.object_]: ...

class ChunkedWKBArray:
    def __array__(self) -> NDArray[np.object_]: ...
    def __eq__(self, other: Self) -> bool: ...
    def __len__(self) -> int: ...
    def chunk(self, i: int) -> WKBArray: ...
    def chunks(self) -> List[WKBArray]: ...
    @classmethod
    def from_arrow_arrays(cls, input: Sequence[ArrowArrayExportable]) -> Self: ...
    @classmethod
    def from_shapely(cls, input, *, chunk_size: int = 65536) -> Self: ...
    def num_chunks(self) -> int: ...
    def to_shapely(self) -> NDArray[np.object_]: ...

class ChunkedRectArray:
    def __eq__(self, other: Self) -> bool: ...
    def __len__(self) -> int: ...
    def chunk(self, i: int) -> RectArray: ...
    def chunks(self) -> List[RectArray]: ...
    @classmethod
    def from_arrow_arrays(cls, input: Sequence[ArrowArrayExportable]) -> Self: ...
    def num_chunks(self) -> int: ...

class ChunkedBooleanArray:
    def __eq__(self, other: Self) -> bool: ...
    def __len__(self) -> int: ...
    def chunk(self, i: int) -> BooleanArray: ...
    def chunks(self) -> List[BooleanArray]: ...
    @classmethod
    def from_arrow_arrays(cls, input: Sequence[ArrowArrayExportable]) -> Self: ...
    def num_chunks(self) -> int: ...

# class ChunkedFloat16Array:
#     def __eq__(self, other: Self) -> bool: ...
#     def __len__(self) -> int: ...
#     def chunk(self, i: int) -> Float16Array: ...
#     def chunks(self) -> List[Float16Array]: ...
#     def num_chunks(self) -> int: ...

# class ChunkedFloat32Array:
#     def __eq__(self, other: Self) -> bool: ...
#     def __len__(self) -> int: ...
#     def chunk(self, i: int) -> Float32Array: ...
#     def chunks(self) -> List[Float32Array]: ...
#     def num_chunks(self) -> int: ...

class ChunkedFloat64Array:
    def __eq__(self, other: Self) -> bool: ...
    def __len__(self) -> int: ...
    def chunk(self, i: int) -> Float64Array: ...
    def chunks(self) -> List[Float64Array]: ...
    def num_chunks(self) -> int: ...

# class ChunkedInt16Array:
#     def __eq__(self, other: Self) -> bool: ...
#     def __len__(self) -> int: ...
#     def chunk(self, i: int) -> Int16Array: ...
#     def chunks(self) -> List[Int16Array]: ...
#     def num_chunks(self) -> int: ...

# class ChunkedInt32Array:
#     def __eq__(self, other: Self) -> bool: ...
#     def __len__(self) -> int: ...
#     def chunk(self, i: int) -> Int32Array: ...
#     def chunks(self) -> List[Int32Array]: ...
#     def num_chunks(self) -> int: ...

# class ChunkedInt64Array:
#     def __eq__(self, other: Self) -> bool: ...
#     def __len__(self) -> int: ...
#     def chunk(self, i: int) -> Int64Array: ...
#     def chunks(self) -> List[Int64Array]: ...
#     def num_chunks(self) -> int: ...

# class ChunkedInt8Array:
#     def __eq__(self, other: Self) -> bool: ...
#     def __len__(self) -> int: ...
#     def chunk(self, i: int) -> Int8Array: ...
#     def chunks(self) -> List[Int8Array]: ...
#     def num_chunks(self) -> int: ...

# class ChunkedLargeStringArray:
#     def __eq__(self, other: Self) -> bool: ...
#     def __len__(self) -> int: ...
#     def chunk(self, i: int) -> LargeStringArray: ...
#     def chunks(self) -> List[LargeStringArray]: ...
#     def num_chunks(self) -> int: ...

# class ChunkedStringArray:
#     def __eq__(self, other: Self) -> bool: ...
#     def __len__(self) -> int: ...
#     def chunk(self, i: int) -> StringArray: ...
#     def chunks(self) -> List[StringArray]: ...
#     def num_chunks(self) -> int: ...

# class ChunkedUInt16Array:
#     def __eq__(self, other: Self) -> bool: ...
#     def __len__(self) -> int: ...
#     def chunk(self, i: int) -> UInt16Array: ...
#     def chunks(self) -> List[UInt16Array]: ...
#     def num_chunks(self) -> int: ...

# class ChunkedUInt32Array:
#     def __eq__(self, other: Self) -> bool: ...
#     def __len__(self) -> int: ...
#     def chunk(self, i: int) -> UInt32Array: ...
#     def chunks(self) -> List[UInt32Array]: ...
#     def num_chunks(self) -> int: ...

# class ChunkedUInt64Array:
#     def __eq__(self, other: Self) -> bool: ...
#     def __len__(self) -> int: ...
#     def chunk(self, i: int) -> UInt64Array: ...
#     def chunks(self) -> List[UInt64Array]: ...
#     def num_chunks(self) -> int: ...

# class ChunkedUInt8Array:
#     def __eq__(self, other: Self) -> bool: ...
#     def __len__(self) -> int: ...
#     def chunk(self, i: int) -> UInt8Array: ...
#     def chunks(self) -> List[UInt8Array]: ...
#     def num_chunks(self) -> int: ...

class GeoTable:
    def __arrow_c_stream__(
        self, requested_schema: object | None = None
    ) -> Tuple[object, object]: ...
    def __eq__(self, other: Self) -> bool: ...
    @property
    def __geo_interface__(self) -> dict: ...
    def __len__(self) -> int: ...
    def explode(self) -> Self: ...
    @classmethod
    def from_arrow(cls, input: ArrowStreamExportable) -> Self: ...
    @classmethod
    def from_geopandas(cls, input: gpd.GeoDataFrame) -> Self: ...
    @property
    def geometry(
        self,
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
    @property
    def num_columns(self) -> int: ...
    def to_geopandas(self) -> gpd.GeoDataFrame: ...

# Operations
def area(input: ArrowArrayExportable) -> Float64Array: ...
def signed_area(input: ArrowArrayExportable) -> Float64Array: ...
def center(input: ArrowArrayExportable) -> PointArray: ...
def centroid(input: ArrowArrayExportable) -> PointArray: ...
def chaikin_smoothing(
    input: ArrowArrayExportable, n_iterations: int
) -> LineStringArray | PolygonArray | MultiLineStringArray | MultiPolygonArray: ...
def chamberlain_duquette_unsigned_area(input: ArrowArrayExportable) -> Float64Array: ...
def chamberlain_duquette_signed_area(input: ArrowArrayExportable) -> Float64Array: ...
def convex_hull(input: ArrowArrayExportable) -> PolygonArray: ...
def densify(
    input: ArrowArrayExportable, max_distance: float
) -> LineStringArray | PolygonArray | MultiLineStringArray | MultiPolygonArray: ...
def envelope(input: ArrowArrayExportable) -> RectArray: ...
def is_empty(input: ArrowArrayExportable) -> BooleanArray: ...
def geodesic_area_signed(input: ArrowArrayExportable) -> Float64Array: ...
def geodesic_area_unsigned(input: ArrowArrayExportable) -> Float64Array: ...
def geodesic_perimeter(input: ArrowArrayExportable) -> Float64Array: ...
def simplify(
    input: ArrowArrayExportable, epsilon: float
) -> (
    PointArray
    | LineStringArray
    | PolygonArray
    | MultiPointArray
    | MultiLineStringArray
    | MultiPolygonArray
): ...
def simplify_vw(
    input: ArrowArrayExportable, epsilon: float
) -> (
    PointArray
    | LineStringArray
    | PolygonArray
    | MultiPointArray
    | MultiLineStringArray
    | MultiPolygonArray
): ...

# I/O
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
def to_wkb(input: ArrowArrayExportable) -> WKBArray: ...
def read_csv(
    file: str | Path | BinaryIO,
    geometry_column_name: str,
    *,
    batch_size: int = 65536,
) -> GeoTable: ...
def read_flatgeobuf(
    file: Union[str, Path, BinaryIO], batch_size: int = 65536
) -> GeoTable: ...
def read_geojson(
    file: Union[str, Path, BinaryIO], batch_size: int = 65536
) -> GeoTable: ...
def read_geojson_lines(
    file: Union[str, Path, BinaryIO], batch_size: int = 65536
) -> GeoTable: ...
def read_parquet(path: str, batch_size: int = 65536) -> GeoTable: ...
def read_postgis(connection_url: str, sql: str) -> Optional[GeoTable]: ...
async def read_postgis_async(connection_url: str, sql: str) -> Optional[GeoTable]: ...
def write_csv(
    table: ArrowStreamExportable, file: str | Path | BinaryIO
) -> GeoTable: ...
def write_flatgeobuf(
    table: ArrowStreamExportable,
    file: str | Path | BinaryIO,
    *,
    write_index: bool = True,
) -> GeoTable: ...
def write_geojson(
    table: ArrowStreamExportable, file: Union[str, Path, BinaryIO]
) -> GeoTable: ...

# Interop
def read_pyogrio(
    path_or_buffer,
    /,
    layer=None,
    encoding=None,
    columns=None,
    read_geometry=True,
    force_2d=False,
    skip_features=0,
    max_features=None,
    where=None,
    bbox=None,
    mask=None,
    fids=None,
    sql=None,
    sql_dialect=None,
    return_fids=False,
    batch_size=65536,
    **kwargs,
) -> GeoTable: ...
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
def to_shapely(input: ArrowArrayExportable) -> NDArray[np.object_]: ...
