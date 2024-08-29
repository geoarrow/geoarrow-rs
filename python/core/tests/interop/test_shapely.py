import geoarrow.rust.core as gars
import geodatasets
import geopandas as gpd
import numpy as np
from pyproj import CRS
import pytest
import shapely
from geoarrow.rust.core import get_crs

nybb_path = geodatasets.get_path("nybb")


def test_from_shapely():
    gdf = gpd.read_file(nybb_path)
    shapely_orig = np.array(gdf.geometry)
    ga_arr = gars.from_shapely(shapely_orig)
    shapely_back = ga_arr.to_shapely()
    assert np.all(shapely_orig == shapely_back)
    assert isinstance(ga_arr, gars.MultiPolygonArray)
    ga_arr_back = gars.from_shapely(shapely_back)
    assert isinstance(ga_arr_back, gars.MultiPolygonArray)
    assert ga_arr == ga_arr_back


def test_from_shapely_crs():
    points = shapely.points([1, 2, 3], [4, 5, 6])
    crs = CRS.from_epsg(4326)
    array = gars.from_shapely(points, crs)
    assert get_crs(array) == crs


def test_from_shapely_chunked():
    gdf = gpd.read_file(nybb_path)
    shapely_orig = np.array(gdf.geometry)
    chunked_arr = gars.ChunkedMultiPolygonArray.from_shapely(shapely_orig, chunk_size=2)
    assert chunked_arr.num_chunks() == 3
    assert np.all(shapely_orig == chunked_arr.to_shapely())


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
    ga_arr = gars.from_shapely(mixed_shapely_geoms)
    assert isinstance(ga_arr, gars.MixedGeometryArray)
    assert isinstance(ga_arr[1], gars.MultiPolygon)

    shapely_back = ga_arr.to_shapely()
    assert shapely_back == ga_arr.to_shapely()
