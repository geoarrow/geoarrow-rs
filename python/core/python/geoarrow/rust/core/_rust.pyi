from __future__ import annotations

from pathlib import Path
from typing import (
    Any,
    BinaryIO,
    Dict,
    List,
    Optional,
    Self,
    Sequence,
    Tuple,
    Union,
    overload,
)

from arro3.core import Array, ChunkedArray, RecordBatchReader, Schema, Table
from arro3.core.types import (
    ArrowArrayExportable,
    ArrowSchemaExportable,
    ArrowStreamExportable,
)
from pyproj import CRS

try:
    import numpy as np
    from numpy.typing import NDArray
except ImportError:
    pass

try:
    import geopandas as gpd
except ImportError:
    pass

from .enums import (
    AreaMethod,
    GeoParquetEncoding,
    LengthMethod,
    RotateOrigin,
    SimplifyMethod,
)
from .types import (
    AffineTransform,
    AreaMethodT,
    BboxPaths,
    BroadcastGeometry,
    GeoInterfaceProtocol,
    GeoParquetEncodingT,
    IntFloat,
    LengthMethodT,
    NumpyArrayProtocolf64,
    RotateOriginT,
    SimplifyMethodT,
)

class Geometry:
    def __arrow_c_array__(
        self, requested_schema: object | None = None
    ) -> Tuple[object, object]: ...
    def __eq__(self, other: object) -> bool: ...
    @property
    def __geo_interface__(self) -> dict: ...
    def __repr__(self) -> str: ...
    def _repr_svg_(self) -> str: ...

class GeometryArray:
    def __arrow_c_array__(
        self, requested_schema: object | None = None
    ) -> Tuple[object, object]: ...
    def __eq__(self, other: object) -> bool: ...
    @property
    def __geo_interface__(self) -> dict: ...
    def __getitem__(self, key: int) -> Geometry: ...
    def __len__(self) -> int: ...
    def __repr__(self) -> str: ...
    @classmethod
    def from_arrow(cls, input: ArrowArrayExportable) -> Self: ...

class ChunkedGeometryArray:
    def __arrow_c_stream__(self, requested_schema: object | None = None) -> object: ...
    def __eq__(self, other: object) -> bool: ...
    def __getitem__(self, key: int) -> Geometry: ...
    def __len__(self) -> int: ...
    def __repr__(self) -> str: ...
    def chunk(self, i: int) -> GeometryArray: ...
    def chunks(self) -> List[GeometryArray]: ...
    @classmethod
    def from_arrow_arrays(cls, input: Sequence[ArrowArrayExportable]) -> Self: ...
    def num_chunks(self) -> int: ...

# Top-level array/chunked array functions

@overload
def affine_transform(
    input: ArrowArrayExportable, transform: AffineTransform
) -> GeometryArray: ...
@overload
def affine_transform(
    input: ArrowStreamExportable, transform: AffineTransform
) -> ChunkedGeometryArray: ...
def affine_transform(
    input: ArrowArrayExportable | ArrowStreamExportable, transform: AffineTransform
) -> GeometryArray | ChunkedGeometryArray: ...
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
def center(input: ArrowArrayExportable) -> GeometryArray: ...
@overload
def center(input: ArrowStreamExportable) -> ChunkedGeometryArray: ...
def center(
    input: ArrowArrayExportable | ArrowStreamExportable,
) -> GeometryArray | ChunkedGeometryArray: ...
@overload
def centroid(input: ArrowArrayExportable) -> GeometryArray: ...
@overload
def centroid(input: ArrowStreamExportable) -> ChunkedGeometryArray: ...
def centroid(
    input: ArrowArrayExportable | ArrowStreamExportable,
) -> GeometryArray | ChunkedGeometryArray: ...
@overload
def chaikin_smoothing(
    input: ArrowArrayExportable, n_iterations: int
) -> GeometryArray: ...
@overload
def chaikin_smoothing(
    input: ArrowStreamExportable, n_iterations: int
) -> ChunkedGeometryArray: ...
def chaikin_smoothing(
    input: ArrowArrayExportable | ArrowStreamExportable,
    n_iterations: int,
) -> GeometryArray | ChunkedGeometryArray: ...
@overload
def convex_hull(input: ArrowArrayExportable) -> GeometryArray: ...
@overload
def convex_hull(input: ArrowStreamExportable) -> ChunkedGeometryArray: ...
def convex_hull(
    input: ArrowArrayExportable | ArrowStreamExportable,
) -> GeometryArray | ChunkedGeometryArray: ...
def densify(input: ArrowArrayExportable, max_distance: float) -> GeometryArray: ...
@overload
def envelope(input: ArrowArrayExportable) -> GeometryArray: ...
@overload
def envelope(input: ArrowStreamExportable) -> ChunkedGeometryArray: ...
def envelope(
    input: ArrowArrayExportable | ArrowStreamExportable,
) -> GeometryArray | ChunkedGeometryArray: ...
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
) -> GeometryArray: ...
@overload
def line_interpolate_point(
    input: ArrowStreamExportable,
    fraction: float | int | ArrowStreamExportable,
) -> ChunkedGeometryArray: ...
def line_interpolate_point(
    input: ArrowArrayExportable | ArrowStreamExportable,
    fraction: float
    | int
    | ArrowArrayExportable
    | ArrowStreamExportable
    | NumpyArrayProtocolf64,
) -> GeometryArray | ChunkedGeometryArray: ...
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
) -> GeometryArray: ...
@overload
def polylabel(
    input: ArrowStreamExportable,
    tolerance: float,
) -> ChunkedGeometryArray: ...
def polylabel(
    input: ArrowArrayExportable | ArrowStreamExportable,
    tolerance: float,
) -> GeometryArray | ChunkedGeometryArray: ...
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
def rotate(
    geom: ArrowArrayExportable,
    angle: float,
    *,
    origin: RotateOrigin | RotateOriginT | tuple[float, float],
) -> GeometryArray: ...
@overload
def rotate(
    geom: ArrowStreamExportable,
    angle: float,
    *,
    origin: RotateOrigin | RotateOriginT | tuple[float, float],
) -> ChunkedGeometryArray: ...
def rotate(
    geom: ArrowArrayExportable | ArrowStreamExportable,
    angle: float,
    *,
    origin: RotateOrigin | RotateOriginT | tuple[float, float],
) -> GeometryArray | ChunkedGeometryArray: ...
@overload
def scale(geom: ArrowArrayExportable, xfact: float, yfact: float) -> GeometryArray: ...
@overload
def scale(
    geom: ArrowStreamExportable, xfact: float, yfact: float
) -> ChunkedGeometryArray: ...
def scale(
    geom: ArrowArrayExportable | ArrowStreamExportable, xfact: float, yfact: float
) -> GeometryArray | ChunkedGeometryArray: ...
@overload
def simplify(
    input: ArrowArrayExportable,
    epsilon: float,
    *,
    method: SimplifyMethod | SimplifyMethodT = SimplifyMethod.RDP,
) -> GeometryArray: ...
@overload
def simplify(
    input: ArrowStreamExportable,
    epsilon: float,
    *,
    method: SimplifyMethod | SimplifyMethodT = SimplifyMethod.RDP,
) -> ChunkedGeometryArray: ...
def simplify(
    input: ArrowArrayExportable | ArrowStreamExportable,
    epsilon: float,
    *,
    method: SimplifyMethod | SimplifyMethodT = SimplifyMethod.RDP,
) -> GeometryArray | ChunkedGeometryArray: ...
@overload
def skew(geom: ArrowArrayExportable, xs: float, ys: float) -> GeometryArray: ...
@overload
def skew(geom: ArrowStreamExportable, xs: float, ys: float) -> ChunkedGeometryArray: ...
def skew(
    geom: ArrowArrayExportable | ArrowStreamExportable, xs: float, ys: float
) -> GeometryArray | ChunkedGeometryArray: ...
@overload
def translate(
    geom: ArrowArrayExportable, xoff: float, yoff: float
) -> GeometryArray: ...
@overload
def translate(
    geom: ArrowStreamExportable, xoff: float, yoff: float
) -> ChunkedGeometryArray: ...
def translate(
    geom: ArrowArrayExportable | ArrowStreamExportable, xoff: float, yoff: float
) -> GeometryArray | ChunkedGeometryArray: ...
def total_bounds(
    input: ArrowArrayExportable | ArrowStreamExportable,
) -> Tuple[float, float, float, float]: ...

# Top-level table functions

def explode(input: ArrowStreamExportable) -> Table: ...
def geometry_col(table: ArrowStreamExportable) -> ChunkedGeometryArray: ...

# I/O

class ParquetFile:
    def __init__(self, path: str, fs: ObjectStore) -> None: ...
    @property
    def num_rows(self) -> int: ...
    @property
    def num_row_groups(self) -> int: ...
    @property
    def schema_arrow(self) -> Schema: ...
    def crs(self, column_name: str | None = None) -> CRS: ...
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
    ) -> GeometryArray: ...
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
    @property
    def schema_arrow(self) -> Schema: ...
    def crs(self, column_name: str | None = None) -> CRS: ...
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
    path: Union[str, Path, BinaryIO],
    *,
    fs: Optional[ObjectStore] = None,
    batch_size: int = 65536,
) -> Table: ...
async def read_parquet_async(
    path: Union[str, Path, BinaryIO],
    *,
    fs: Optional[ObjectStore] = None,
    batch_size: int = 65536,
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
    file: Union[str, Path, BinaryIO],
    *,
    encoding: GeoParquetEncoding | GeoParquetEncodingT = GeoParquetEncoding.WKB,
) -> None: ...

# Interop
def from_ewkb(input: ArrowArrayExportable) -> GeometryArray: ...
def from_geopandas(input: gpd.GeoDataFrame) -> Table: ...
def from_shapely(input, *, crs: Any | None = None) -> GeometryArray: ...
def from_wkb(input: ArrowArrayExportable) -> GeometryArray: ...
def from_wkt(input: ArrowArrayExportable) -> GeometryArray: ...
def to_geopandas(input: ArrowStreamExportable) -> gpd.GeoDataFrame: ...
def to_shapely(
    input: ArrowArrayExportable | ArrowStreamExportable,
) -> NDArray[np.object_]: ...
def to_wkb(input: ArrowArrayExportable) -> GeometryArray: ...
