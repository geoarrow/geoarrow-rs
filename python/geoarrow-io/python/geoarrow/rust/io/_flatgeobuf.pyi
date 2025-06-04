from __future__ import annotations

from pathlib import Path
from typing import BinaryIO, Optional, Tuple, Union

from arro3.core import Table
from arro3.core.types import ArrowStreamExportable
from geoarrow.rust.core.enums import CoordType
from geoarrow.rust.core.types import CoordTypeT

def read_flatgeobuf(
    file: Union[str, Path, BinaryIO],
    *,
    store: Optional[ObjectStore] = None,
    batch_size: int = 65536,
    bbox: Tuple[float, float, float, float] | None = None,
) -> Table:
    """
    Read a FlatGeobuf file from a path on disk or a remote location into an Arrow Table.

    Example:

    Reading from a local path:

    ```py
    from geoarrow.rust.io import read_flatgeobuf
    table = read_flatgeobuf("path/to/file.fgb")
    ```

    Reading from a Python file object:

    ```py
    from geoarrow.rust.io import read_flatgeobuf

    with open("path/to/file.fgb", "rb") as file:
        table = read_flatgeobuf(file)
    ```

    Reading from an HTTP(S) url:

    ```py
    from geoarrow.rust.io import read_flatgeobuf

    url = "http://flatgeobuf.org/test/data/UScounties.fgb"
    table = read_flatgeobuf(url)
    ```

    Reading from a remote file on an S3 bucket.

    ```py
    from geoarrow.rust.io import ObjectStore, read_flatgeobuf

    options = {
        "aws_access_key_id": "...",
        "aws_secret_access_key": "...",
        "aws_region": "..."
    }
    store = ObjectStore('s3://bucket', options=options)
    table = read_flatgeobuf("path/in/bucket.fgb", store=store)
    ```

    Args:
        file: the path to the file or a Python file object in binary read mode.

    Other args:
        store: an ObjectStore instance for this url. This is required only if the file is at a remote
            location.
        batch_size: the number of rows to include in each internal batch of the table.
        bbox: A spatial filter for reading rows, of the format (minx, miny, maxx, maxy).
            If set to `None`, no spatial filtering will be performed.

    Returns:
        Table from FlatGeobuf file.
    """

async def read_flatgeobuf_async(
    path: str,
    *,
    store: Optional[ObjectStore] = None,
    batch_size: int = 65536,
    bbox: Tuple[float, float, float, float] | None = None,
    coord_type: CoordType | CoordTypeT | None = None,
) -> Table:
    """
    Read a FlatGeobuf file from a url into an Arrow Table.

    Example:

    Reading from an HTTP(S) url:

    ```py
    from geoarrow.rust.io import read_flatgeobuf_async

    url = "http://flatgeobuf.org/test/data/UScounties.fgb"
    table = await read_flatgeobuf_async(url)
    ```

    Reading from an S3 bucket:

    ```py
    from geoarrow.rust.io import ObjectStore, read_flatgeobuf_async

    options = {
        "aws_access_key_id": "...",
        "aws_secret_access_key": "...",
        "aws_region": "..."
    }
    store = ObjectStore('s3://bucket', options=options)
    table = await read_flatgeobuf_async("path/in/bucket.fgb", store=store)
    ```

    Args:
        path: the url or relative path to a remote FlatGeobuf file. If an argument is passed for
            `store`, this should be a path fragment relative to the root passed to the `ObjectStore`
            constructor.

    Other args:
        store: an ObjectStore instance for this url. This is required for non-HTTP urls.
        batch_size: the number of rows to include in each internal batch of the table.
        bbox: A spatial filter for reading rows, of the format (minx, miny, maxx, maxy). If set to
            `None`, no spatial filtering will be performed.
        coord_type: The GeoArrow coordinate variant to use.

    Returns:
        Table from FlatGeobuf file.
    """

def write_flatgeobuf(
    table: ArrowStreamExportable,
    file: str | Path | BinaryIO,
    *,
    write_index: bool = True,
    promote_to_multi: bool = True,
    title: str | None = None,
    description: str | None = None,
    metadata: str | None = None,
    name: str | None = None,
) -> None:
    """
    Write to a FlatGeobuf file on disk.

    Args:
        table: the Arrow RecordBatch, Table, or RecordBatchReader to write.
        file: the path to the file or a Python file object in binary write mode.

    Other args:
        write_index: whether to write a spatial index in the FlatGeobuf file. Defaults to True.
        title: Dataset title. Defaults to `None`.
        description: Dataset description (intended for free form long text).
        metadata: Dataset metadata (intended to be application specific).
        name: the string passed to `FgbWriter::create` and is what OGR observes as the layer name of the file. By default, this will try to use the file name, but can be overrided.

    """
