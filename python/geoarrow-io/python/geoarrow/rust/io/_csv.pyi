from pathlib import Path
from typing import BinaryIO

from arro3.core import Table
from arro3.core.types import ArrowStreamExportable

def read_csv(
    file: str | Path | BinaryIO,
    geometry_column_name: str,
    *,
    batch_size: int = 65536,
) -> Table:
    """
    Read a CSV file from a path on disk into a Table.

    Args:
        file: the path to the file or a Python file object in binary read mode.
        geometry_column_name: the name of the geometry column within the CSV.
        batch_size: the number of rows to include in each internal batch of the table.

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
