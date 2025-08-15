import numpy as np
from arro3.core import ArrayReader, Field, RecordBatch, RecordBatchReader, Schema, Table
from geoarrow.rust.core import get_crs, point, points
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


def test_pyproj_crs():
    crs = CRS.from_epsg(4326)
    typ = point("xy", crs=crs)
    assert typ.crs == crs


def test_crs_inference():
    s = "epsg:4326"
    expected_crs = CRS.from_user_input(s)
    typ = point("xy", crs=s)
    assert typ.crs == expected_crs

    projjson_str = expected_crs.to_json()
    typ = point("xy", crs=projjson_str)
    assert typ.crs == expected_crs

    projjson_dict = expected_crs.to_json_dict()
    typ = point("xy", crs=projjson_dict)
    assert typ.crs == expected_crs


def test_no_crs():
    typ = point("xy")
    assert typ.crs is None
