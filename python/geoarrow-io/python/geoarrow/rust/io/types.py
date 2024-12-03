from __future__ import annotations

from typing import Literal, Sequence, TypedDict


GeoParquetEncodingT = Literal["wkb", "native"]
"""Acceptable strings to be passed into the `encoding` parameter for
[`write_parquet`][geoarrow.rust.io.write_parquet].
"""


class BboxCovering(TypedDict):
    """Column names for the per-row bounding box covering used in spatial partitioning.

    The spatial partitioning defined in GeoParquet 1.1 allows for a [`"covering"`
    field](https://github.com/opengeospatial/geoparquet/blob/v1.1.0/format-specs/geoparquet.md#covering).
    The covering should be four float columns that represent the bounding box of each
    row of the data.

    As of GeoParquet 1.1, this metadata is included in the Parquet file itself, but this
    typed dict can be used with spatially-partitioned GeoParquet datasets that do not
    write GeoParquet 1.1 metadata. Providing this information is unnecessary for
    GeoParquet 1.1 files with included covering information.
    """

    xmin: Sequence[str]
    """The path to the xmin bounding box column."""

    ymin: Sequence[str]
    """The path to the ymin bounding box column."""

    xmax: Sequence[str]
    """The path to the xmax bounding box column."""

    ymax: Sequence[str]
    """The path to the ymax bounding box column."""
