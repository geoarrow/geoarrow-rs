from pathlib import Path
from typing import BinaryIO, Literal, overload

from arro3.core import RecordBatchReader, Table
from arro3.core.types import ArrowStreamExportable
from geoarrow.rust.core.enums import CoordType
from geoarrow.rust.core.types import CoordTypeT

@overload
def read_csv(
    file: str | Path | BinaryIO,
    *,
    geometry_name: str | None = None,
    downcast_geometry: Literal[True] = True,
    batch_size: int = 65536,
    coord_type: CoordType | CoordTypeT | None = None,
    has_header: bool = True,
    max_records: int | None = None,
    delimiter: str | None = None,
    escape: str | None = None,
    quote: str | None = None,
    terminator: str | None = None,
    comment: str | None = None,
) -> Table: ...
@overload
def read_csv(
    file: str | Path | BinaryIO,
    *,
    geometry_name: str | None = None,
    downcast_geometry: Literal[False],
    batch_size: int = 65536,
    coord_type: CoordType | CoordTypeT | None = None,
    has_header: bool = True,
    max_records: int | None = None,
    delimiter: str | None = None,
    escape: str | None = None,
    quote: str | None = None,
    terminator: str | None = None,
    comment: str | None = None,
) -> RecordBatchReader: ...
def read_csv(
    file: str | Path | BinaryIO,
    *,
    geometry_name: str | None = None,
    downcast_geometry: bool = True,
    batch_size: int = 65536,
    coord_type: CoordType | CoordTypeT | None = None,
    has_header: bool = True,
    max_records: int | None = None,
    delimiter: str | None = None,
    escape: str | None = None,
    quote: str | None = None,
    terminator: str | None = None,
    comment: str | None = None,
) -> RecordBatchReader | Table:
    '''
    Read a CSV file with a WKT-encoded geometry column.

    Example:

    ```py
    csv_text = """
    address,type,datetime,report location,incident number
    904 7th Av,Car Fire,05/22/2019 12:55:00 PM,POINT (-122.329051 47.6069),F190051945
    9610 53rd Av S,Aid Response,05/22/2019 12:55:00 PM,POINT (-122.266529 47.515984),F190051946"
    """

    table = read_csv(BytesIO(csv_text.encode()), geometry_name="report location")
    ```

    Or, if you'd like to stream the data, you can pass `downcast_geometry=False`:

    ```py
    record_batch_reader = read_csv(
        path_to_csv,
        geometry_name="report location",
        downcast_geometry=False,
        batch_size=100_000,
    )
    for record_batch in record_batch_reader:
        # Use each record batch.
    ```

    Args:
        file: the path to the file or a Python file object in binary read mode.

    Other args:
        geometry_name: the name of the geometry column within the CSV. By default, will look for a column named "geometry", case insensitive.
        downcast_geometry: Whether to simplify the type of the geometry column. When `downcast_geometry` is `False`, the GeoArrow geometry column is of type "Geometry", which is fully generic. When `downcast_geometry` is `True`, the GeoArrow geometry column will be simplified to its most basic representation. That is, if the table only includes points, the GeoArrow geometry column will be converted to a Point-type array.

            Downcasting is only possible when all chunks have been loaded into memory.
            Use `downcast_geometry=False` if you would like to iterate over batches of
            the table, without loading all of them into memory at once.
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
        A `Table` if `downcast_geometry` is `True` (the default). If `downcast_geometry`
        is `False`, returns a `RecordBatchReader`, enabling streaming processing.
    '''

def write_csv(table: ArrowStreamExportable, file: str | Path | BinaryIO) -> None:
    """
    Write a Table to a CSV file on disk.

    Args:
        table: the Arrow RecordBatch, Table, or RecordBatchReader to write.
        file: the path to the file or a Python file object in binary write mode.

    Returns:
        None
    """
