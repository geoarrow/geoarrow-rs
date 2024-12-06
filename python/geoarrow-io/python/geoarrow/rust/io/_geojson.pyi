from __future__ import annotations

from pathlib import Path
from typing import BinaryIO, Union

from arro3.core import Table
from arro3.core.types import ArrowStreamExportable

def read_geojson(file: Union[str, Path, BinaryIO], *, batch_size: int = 65536) -> Table:
    """
    Read a GeoJSON file from a path on disk into an Arrow Table.

    Args:
        file: the path to the file or a Python file object in binary read mode.
        batch_size: the number of rows to include in each internal batch of the table.

    Returns:
        Table from GeoJSON file.
    """

def read_geojson_lines(
    file: Union[str, Path, BinaryIO], *, batch_size: int = 65536
) -> Table:
    """
    Read a newline-delimited GeoJSON file from a path on disk into an Arrow Table.

    This expects a GeoJSON Feature on each line of a text file, with a newline character separating
    each Feature.

    Args:
        file: the path to the file or a Python file object in binary read mode.
        batch_size: the number of rows to include in each internal batch of the table.

    Returns:
        Table from GeoJSON file.
    """

def write_geojson(
    table: ArrowStreamExportable, file: Union[str, Path, BinaryIO]
) -> None:
    """
    Write to a GeoJSON file on disk.

    Note that the GeoJSON specification mandates coordinates to be in the WGS84 (EPSG:4326)
    coordinate system, but this function will not automatically reproject into WGS84 for you.

    Args:
        table: the Arrow RecordBatch, Table, or RecordBatchReader to write.
        file: the path to the file or a Python file object in binary write mode.

    Returns:
        None
    """

def write_geojson_lines(
    table: ArrowStreamExportable, file: Union[str, Path, BinaryIO]
) -> None:
    """
    Write to a newline-delimited GeoJSON file on disk.

    Note that the GeoJSON specification mandates coordinates to be in the WGS84 (EPSG:4326)
    coordinate system, but this function will not automatically reproject into WGS84 for you.

    Args:
        table: the Arrow RecordBatch, Table, or RecordBatchReader to write.
        file: the path to the file or a Python file object in binary write mode.

    Returns:
        None
    """
