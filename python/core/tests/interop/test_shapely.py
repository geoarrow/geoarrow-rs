import geodatasets
import geopandas as gpd
import numpy as np
import pytest
import shapely
import shapely.testing
from geoarrow.rust.core import from_shapely, get_crs, to_shapely
from pyproj import CRS

nybb_path = geodatasets.get_path("nybb")


def test_from_shapely():
    gdf = gpd.read_file(nybb_path)
    shapely_orig = np.array(gdf.geometry)
    ga_arr = from_shapely(shapely_orig)
    # assert isinstance(ga_arr, gars.MultiPolygonArray)

    shapely_rt = to_shapely(ga_arr)
    shapely.testing.assert_geometries_equal(shapely_orig, shapely_rt)
    _ga_arr_back = from_shapely(shapely_rt)
    # assert isinstance(ga_arr_back, gars.MultiPolygonArray)
    # TODO: implement eq
    # assert ga_arr == ga_arr_back


def test_from_shapely_crs():
    points = shapely.points([1, 2, 3], [4, 5, 6])
    crs = CRS.from_epsg(4326)
    array = from_shapely(points, crs=crs)
    assert get_crs(array) == crs


# from_shapely does not currently support chunked arrays
# def test_from_shapely_chunked():
#     gdf = gpd.read_file(nybb_path)
#     shapely_orig = np.array(gdf.geometry)
#     chunked_arr = gars.ChunkedMultiPolygonArray.from_shapely(shapely_orig, chunk_size=2)
#     assert chunked_arr.num_chunks() == 3
#     shapely.testing.assert_geometries_equal(shapely_orig, to_shapely(chunked_arr))


@pytest.mark.xfail(
    reason=(
        "WKB -> Mixed array current parses as a mixed array, "
        " and the MultiPoint is never downcasted back to a Point."
    )
)
def test_from_shapely_mixed():
    point = shapely.points(1, 1)
    multi_polygon = gpd.read_file(nybb_path).geometry.iloc[0]
    mixed_shapely_geoms = np.array([point, multi_polygon])
    ga_arr = from_shapely(mixed_shapely_geoms)
    # TODO: should be
    # assert isinstance(ga_arr, gars.MixedGeometryArray)
    # assert isinstance(ga_arr[1], gars.MultiPolygon)

    shapely_back = to_shapely(ga_arr)
    assert point == shapely_back[0]
