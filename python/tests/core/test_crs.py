import numpy as np
from arro3.core import ArrayReader, Field, RecordBatch, RecordBatchReader, Schema, Table
from geoarrow.rust.core import get_crs, points
from pyproj import CRS


def test_get_crs():
    arr = np.array([[1, 2, 3], [4, 5, 6]], dtype=np.float64)
    crs = CRS.from_epsg(4326)
    arr = points(arr, crs=crs)

    field = Field.from_arrow(arr.type).with_name("geometry")
    schema = Schema([field])
    batch = RecordBatch.from_arrays([arr], schema=schema)
    table = Table.from_batches([batch], schema=schema)

    # Schema
    assert get_crs(schema) == crs

    # Field
    assert get_crs(schema.field("geometry")) == crs

    # Table
    assert get_crs(table) == crs

    # ChunkedArray
    assert get_crs(table["geometry"]) == crs

    # RecordBatch
    assert get_crs(batch) == crs

    # Array
    assert get_crs(batch["geometry"]) == crs

    # RecordBatchReader
    reader = RecordBatchReader.from_batches(table.schema, [batch])
    assert get_crs(reader) == crs
    assert not reader.closed

    # ArrayReader
    array = batch["geometry"]
    reader = ArrayReader.from_arrays(array.field, [array])
    assert get_crs(reader) == crs
    assert not reader.closed
