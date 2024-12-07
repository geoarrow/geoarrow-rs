from pathlib import Path
from typing import BinaryIO

from arro3.core import Table
from arro3.core.types import ArrowStreamExportable
from geoarrow.rust.core.enums import CoordType
from geoarrow.rust.core.types import CoordTypeT

def read_csv(
    file: str | Path | BinaryIO,
    *,
    geometry_name: str | None = None,
    batch_size: int = 65536,
    coord_type: CoordType | CoordTypeT | None = None,
    has_header: bool = True,
    max_records: int | None = None,
    delimiter: str | None = None,
    escape: str | None = None,
    quote: str | None = None,
    terminator: str | None = None,
    comment: str | None = None,
) -> Table:
    """
    Read a CSV file from a path on disk into a Table.

    Args:
        file: the path to the file or a Python file object in binary read mode.

    Other args:
        geometry_name: the name of the geometry column within the CSV.
        batch_size: the number of rows to include in each internal batch of the table.
        coord_type: The coordinate type. Defaults to None.
        has_header: Set whether the CSV file has a header. Defaults to `True`.
        max_records: The maximum number of records to read to infer schema. Defaults to
            `None`.
        delimiter: Set the CSV file's column delimiter as a byte character. Defaults to
            `None`.
        escape: Set the CSV escape character. Defaults to `None`.
        quote: Set the CSV quote character. Defaults to `None`.
        terminator: Set the line terminator. Defaults to `None`.
        comment: Set the comment character. Defaults to `None`.

    Returns:
        Table from CSV file.
    """

def write_csv(table: ArrowStreamExportable, file: str | Path | BinaryIO) -> None:
    """
    Write a Table to a CSV file on disk.

    Args:
        table: the Arrow RecordBatch, Table, or RecordBatchReader to write.
        file: the path to the file or a Python file object in binary write mode.

    Returns:
        None
    """
