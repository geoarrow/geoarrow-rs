from __future__ import annotations
import json
from typing import cast

from pyproj.crs.crs import CRS
from arro3.core.types import (
    ArrowArrayExportable,
    ArrowSchemaExportable,
    ArrowStreamExportable,
)
from arro3.core import Field, Schema, ArrayReader


def get_crs(
    data: ArrowArrayExportable | ArrowStreamExportable | ArrowSchemaExportable,
    /,
    column: str | None = None,
) -> CRS | None:
    """Get the CRS from a GeoArrow object.

    Args:
        data: A GeoArrow object. This can be an Array, ChunkedArray, ArrayReader, RecordBatchReader, Table, Field, or Schema.
        column: The name of the geometry column to retrieve, if there's more than one. For Schema, Table, and RecordBatchReader inputs, there may be more than one geometry column included. If there are multiple geometry columns, you must pass this `column` parameter. If there is only one geometry column, it will be inferred. Defaults to None.

    Raises:
        ValueError: If no geometry column could be found.

    Returns:
        a pyproj CRS object.
    """
    # Check schema pointer first, as it won't consume streams
    if hasattr(data, "__arrow_c_schema__"):
        data = cast(ArrowSchemaExportable, data)

        field = Field.from_arrow(data)
        metadata = field.metadata_str
        arrow_ext = metadata.get("ARROW:extension:name", "")
        if arrow_ext.startswith("geoarrow"):
            return parse_metadata(metadata)

        schema = Schema.from_arrow(data)
        geo_field = get_geometry_field(schema, column=column)
        return parse_metadata(geo_field.metadata_str)

    if hasattr(data, "__arrow_c_array__"):
        data = cast(ArrowArrayExportable, data)

        reader = ArrayReader.from_arrow(data)
        return get_crs(reader.field, column=column)

    if hasattr(data, "__arrow_c_stream__"):
        data = cast(ArrowStreamExportable, data)

        reader = ArrayReader.from_arrow(data)
        return get_crs(reader.field, column=column)

    raise ValueError("Unsupported input.")


def parse_metadata(metadata: dict[str, str]) -> CRS | None:
    ext_meta = json.loads(metadata.get("ARROW:extension:metadata", "{}"))
    crs_val = ext_meta.get("crs")
    return CRS.from_user_input(crs_val) if crs_val is not None else None


def get_geometry_field(schema: Schema, column: str | None = None) -> Field:
    if column is not None:
        return schema.field(column)

    geoarrow_col_idxs: list[int] = []
    for field_idx in range(len(schema)):
        field = schema.field(field_idx)
        metadata = field.metadata_str
        arrow_ext = metadata.get("ARROW:extension:name", "")
        if arrow_ext.startswith("geoarrow"):
            geoarrow_col_idxs.append(field_idx)

    if len(geoarrow_col_idxs) <= 0:
        raise ValueError("No geoarrow columns found.")

    elif len(geoarrow_col_idxs) >= 2:
        raise ValueError(
            "Multiple geoarrow columns found. Choose one with the `column` parameter."
        )

    else:
        return schema.field(geoarrow_col_idxs[0])
