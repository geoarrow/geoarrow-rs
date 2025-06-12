from typing import Literal, overload

from arro3.core.types import (
    ArrowArrayExportable,
    ArrowSchemaExportable,
    ArrowStreamExportable,
)
from ._array import GeoArray
from ._array_reader import GeoArrayReader

@overload
def from_wkb(
    input: ArrowArrayExportable,
    to_type: ArrowSchemaExportable | None = None,
) -> GeoArray: ...
@overload
def from_wkb(
    input: ArrowStreamExportable,
    to_type: ArrowSchemaExportable | None = None,
) -> GeoArrayReader: ...
def from_wkb(
    input: ArrowArrayExportable | ArrowStreamExportable,
    to_type: ArrowSchemaExportable | None = None,
) -> GeoArray | GeoArrayReader:
    """Parse the WKB `input` to the provided data type.

    Args:
        input: Input data to parse.
        to_type: The target data type to parse to. By default, parses to a Geometry type array (the output of `geoarrow.rust.core.geometry`).

    Returns:
        If `input` is an Array-like, a `GeoArray` will be returned. If `input` is a Stream-like (`ChunkedArray` or `ArrayReader`), a `GeoArrayReader` will be returned.
    """

@overload
def from_wkt(
    input: ArrowArrayExportable,
    to_type: ArrowSchemaExportable | None = None,
) -> GeoArray: ...
@overload
def from_wkt(
    input: ArrowStreamExportable,
    to_type: ArrowSchemaExportable | None = None,
) -> GeoArrayReader: ...
def from_wkt(
    input: ArrowArrayExportable | ArrowStreamExportable,
    to_type: ArrowSchemaExportable | None = None,
) -> GeoArray | GeoArrayReader:
    """Parse the WKT `input` to the provided data type.

    Args:
        input: Input data to parse.
        to_type: The target data type to parse to. By default, parses to a Geometry type array (the output of `geoarrow.rust.core.geometry`).

    Returns:
        If `input` is an Array-like, a `GeoArray` will be returned. If `input` is a Stream-like (`ChunkedArray` or `ArrayReader`), a `GeoArrayReader` will be returned.
    """

@overload
def to_wkb(
    input: ArrowArrayExportable,
    wkb_type: Literal["wkb", "large_wkb", "wkb_view"] = "wkb",
) -> GeoArray: ...
@overload
def to_wkb(
    input: ArrowStreamExportable,
    wkb_type: Literal["wkb", "large_wkb", "wkb_view"] = "wkb",
) -> GeoArrayReader: ...
def to_wkb(
    input: ArrowArrayExportable | ArrowStreamExportable,
    wkb_type: Literal["wkb", "large_wkb", "wkb_view"] = "wkb",
) -> GeoArray | GeoArrayReader:
    """Convert `input` to WKB.

    Args:
        input: Input data to parse.
        wkb_type: The target WKB array type to convert to. Can be one of "wkb" (binary array with `i32` offsets), "large_wkb" (binary array with `i64` offsets), or "wkb_view" (binary view array).

    Returns:
        If `input` is an Array-like, a `GeoArray` will be returned. If `input` is a Stream-like (`ChunkedArray` or `ArrayReader`), a `GeoArrayReader` will be returned.
    """

@overload
def to_wkt(
    input: ArrowArrayExportable,
    wkt_type: Literal["wkt", "large_wkt", "wkt_view"] = "wkt",
) -> GeoArray: ...
@overload
def to_wkt(
    input: ArrowStreamExportable,
    wkt_type: Literal["wkt", "large_wkt", "wkt_view"] = "wkt",
) -> GeoArrayReader: ...
def to_wkt(
    input: ArrowArrayExportable | ArrowStreamExportable,
    wkt_type: Literal["wkt", "large_wkt", "wkt_view"] = "wkt",
) -> GeoArray | GeoArrayReader:
    """Convert `input` to WKT.

    Args:
        input: Input data to parse.
        wkt_type: The target WKT array type to convert to. Can be one of "wkt" (string array with `i32` offsets), "large_wkt" (string array with `i64` offsets), or "wkt_view" (string view array).

    Returns:
        If `input` is an Array-like, a `GeoArray` will be returned. If `input` is a Stream-like (`ChunkedArray` or `ArrayReader`), a `GeoArrayReader` will be returned.
    """
