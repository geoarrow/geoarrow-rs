from __future__ import annotations

from pathlib import Path
from typing import BinaryIO, Optional, Sequence, Tuple, Union

from arro3.core import Table
from arro3.core.types import ArrowStreamExportable
from geoarrow.rust.core.types import CoordTypeInput
from obstore.store import ObjectStore

def read_flatgeobuf(
    path: Union[str, Path, BinaryIO],
    *,
    store: Optional[ObjectStore] = None,
    batch_size: int = 65536,
    bbox: Tuple[int | float, int | float, int | float, int | float] | None = None,
    coord_type: CoordTypeInput | None = None,
    use_view_types: bool = True,
    max_scan_records: int | None = 1000,
    read_geometry: bool = True,
    columns: Optional[Sequence[str]] = None,
) -> Table:
    """
    Read a FlatGeobuf file from a path on disk or a remote location into an Arrow Table.

    Example:

    Args:
        path: the path to the file or a Python file object in binary read mode.

    Other Args:
        store: an ObjectStore instance for this url. This is required only if the file
            is at a remote location and if the store cannot be inferred.
        batch_size: the number of rows to include in each internal batch of the table.
        bbox: A spatial filter for reading rows, of the format `(minx, miny, maxx,
            maxy)`. If set to `None`, no spatial filtering will be performed.
        coord_type: The GeoArrow coordinate type to use for generated geometries. The default is to use "separated" coordinates.
        use_view_types: If `True`, load string and binary columns into Arrow string view and binary view data types. These are more efficient but less widely supported than the older string and binary data types. Defaults to `True`.
        max_scan_records: The maximum number of records to scan for schema inference. If set to `None`, all records will be scanned. Defaults to 1000.

            Most FlatGeobuf files have a schema defined in the header metadata. But for
            files that do not have a known schema, we need to scan some initial records
            to infer a schema. Reading will fail if a new property with an unknown name
            is found that was not in the schema. Thus, scanning fewer records will be
            faster, but could fail later if the inferred schema was not complete.
        read_geometry: If `True`, read the geometry column. If `False`, the geometry column will be omitted from the result. Defaults to `True`.
        columns: An optional list of property column names to include in the result.
            This is separate from the geometry column, which you can turn on/off with
            `read_geometry`. If `None`, all columns will be included. Defaults to
            `None`.

    Examples:
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

        Reading from a remote file with specified credentials. You can pass any `store`
        constructed from `obstore`, including from [`S3Store`][obstore.store.S3Store],
        [`GCSStore`][obstore.store.GCSStore], [`AzureStore`][obstore.store.AzureStore],
        [`HTTPStore`][obstore.store.HTTPStore] or
        [`LocalStore`][obstore.store.LocalStore].

        ```py
        from geoarrow.rust.io import read_flatgeobuf
        from obstore.store import S3Store

        store = S3Store(
            "bucket-name",
            access_key_id="...",
            secret_access_key="...",
            region="..."
        )
        table = read_flatgeobuf("path/in/bucket.fgb", store=store)
        ```

    Returns:
        Table from FlatGeobuf file.
    """

async def read_flatgeobuf_async(
    path: str,
    *,
    store: Optional[ObjectStore] = None,
    batch_size: int = 65536,
    bbox: Tuple[int | float, int | float, int | float, int | float] | None = None,
    coord_type: CoordTypeInput | None = None,
    use_view_types: bool = True,
    max_scan_records: int | None = 1000,
    read_geometry: bool = True,
    columns: Optional[Sequence[str]] = None,
) -> Table:
    """
    Read a FlatGeobuf file from a url into an Arrow Table.

    Args:
        path: the url or relative path to a remote FlatGeobuf file. If an argument is
            passed for `store`, this should be a path fragment relative to the prefix of
            the store.

    Other Args:
        store: an ObjectStore instance for this url. This is required only if the file
            is at a remote location and if the store cannot be inferred.
        batch_size: the number of rows to include in each internal batch of the table.
        bbox: A spatial filter for reading rows, of the format `(minx, miny, maxx,
            maxy)`. If set to `None`, no spatial filtering will be performed.
        coord_type: The GeoArrow coordinate type to use for generated geometries. The default is to use "separated" coordinates.
        use_view_types: If `True`, load string and binary columns into Arrow string view and binary view data types. These are more efficient but less widely supported than the older string and binary data types. Defaults to `True`.
        max_scan_records: The maximum number of records to scan for schema inference. If set to `None`, all records will be scanned. Defaults to 1000.

            Most FlatGeobuf files have a schema defined in the header metadata. But for
            files that do not have a known schema, we need to scan some initial records
            to infer a schema. Reading will fail if a new property with an unknown name
            is found that was not in the schema. Thus, scanning fewer records will be
            faster, but could fail later if the inferred schema was not complete.
        read_geometry: If `True`, read the geometry column. If `False`, the geometry column will be omitted from the result. Defaults to `True`.
        columns: An optional list of property column names to include in the result.
            This is separate from the geometry column, which you can turn on/off with
            `read_geometry`. If `None`, all columns will be included. Defaults to
            `None`.

    Examples:
        Reading from an HTTP(S) url:

        ```py
        from geoarrow.rust.io import read_flatgeobuf_async

        url = "http://flatgeobuf.org/test/data/UScounties.fgb"
        table = await read_flatgeobuf_async(url)
        ```

        Reading from an S3 bucket:

        ```py
        from geoarrow.rust.io import ObjectStore, read_flatgeobuf_async
        from obstore.store import S3Store

        store = S3Store(
            "bucket-name",
            access_key_id="...",
            secret_access_key="...",
            region="..."
        )
        table = await read_flatgeobuf_async("path/in/bucket.fgb", store=store)
        ```

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
