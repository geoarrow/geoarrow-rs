from typing import overload

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
