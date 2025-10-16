import geopandas as gpd
import numpy as np
import pytest
import shapely
from arro3.core import Table
from geoarrow.rust.core import GeoArray, geometry_col


def geoarrow_array():
    geoms = shapely.points([1, 2, 3], [4, 5, 6])
    return GeoArray.from_arrow(gpd.GeoSeries(geoms).to_arrow("geoarrow"))


def test_batch_no_geom_cols():
    arr = np.array([1, 2, 3])
    # We should have simpler RecordBatch constructor
    # https://github.com/kylebarron/arro3/issues/418
    batch = Table.from_arrays([arr], names=["no_geom"]).to_batches()[0]
    with pytest.raises(ValueError, match="No geometry columns found"):
        geometry_col(batch)


def test_batch_one_geom_col():
    arr = geoarrow_array()
    batch = Table.from_arrays([arr], names=["geom"]).to_batches()[0]
    fetched_arr = geometry_col(batch)
    assert arr == fetched_arr


def test_batch_two_geom_cols():
    arr = geoarrow_array()
    batch = Table.from_arrays([arr, arr], names=["geom1", "geom2"]).to_batches()[0]
    with pytest.raises(ValueError, match="Multiple geometry columns"):
        geometry_col(batch)

    assert geometry_col(batch, name="geom1") == arr
    assert geometry_col(batch, name="geom2") == arr


def test_geo_array_input():
    arr = geoarrow_array()
    assert arr == geometry_col(arr)


# TODO: implement once we have easy GeoChunkedArray constructor
# def test_geo_chunked_array_input():
#     arr = geoarrow_array()
#     chunked = GeoChunkedArray.from_arrays([arr, arr])
#     assert chunked == geometry_col(chunked)


def test_table_no_geom_cols():
    arr = np.array([1, 2, 3])
    table = Table.from_arrays([arr], names=["no_geom"])
    with pytest.raises(ValueError, match="No geometry columns found"):
        geometry_col(table)


def test_table_one_geom_col():
    arr = geoarrow_array()
    table = Table.from_arrays([arr], names=["geom"])
    assert geometry_col(table).read_next_array() == arr


def test_table_two_geom_cols():
    arr = geoarrow_array()
    table = Table.from_arrays([arr, arr], names=["geom1", "geom2"])
    with pytest.raises(ValueError, match="Multiple geometry columns"):
        geometry_col(table)

    assert geometry_col(table, name="geom1").read_next_array() == arr
    assert geometry_col(table, name="geom2").read_next_array() == arr
