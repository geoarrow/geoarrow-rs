import geodatasets
import geopandas as gpd
import numpy as np
import pyarrow as pa
import pytest
import shapely
from geoarrow.rust.core import GeoArray
from geoarrow.types.type_pyarrow import registered_extension_types
from geoarrow.rust.core import points, geometry


def test_eq():
    geoms = shapely.points([1, 2, 3], [4, 5, 6])
    arr = GeoArray.from_arrow(gpd.GeoSeries(geoms).to_arrow("geoarrow"))
    assert arr == arr

    with registered_extension_types():
        pa_arr = pa.array(arr)
        assert arr == pa_arr
        assert arr == GeoArray.from_arrow(pa_arr)


def test_getitem():
    # Tests both the __getitem__ method and the scalar geo interface in round trip
    # conversion to shapely.
    gdf = gpd.read_file(geodatasets.get_path("ny.bb"))
    arr = GeoArray.from_arrow(gdf.geometry.to_arrow("geoarrow"))
    for i in range(len(arr)):
        assert shapely.geometry.shape(arr[i]).equals(gdf.geometry.iloc[i])  # type: ignore


def test_repr():
    geoms = shapely.points([1, 2, 3], [4, 5, 6])
    arr = GeoArray.from_arrow(gpd.GeoSeries(geoms).to_arrow("geoarrow"))
    assert repr(arr) == 'GeoArray(Point(dimension="XY", coord_type="interleaved"))'


def test_downcast():
    coords = np.array([[1, 4], [2, 5], [3, 6]], dtype=np.float64)

    point_arr = points(coords)
    geometry_array = point_arr.cast(geometry())
    point_arr2 = geometry_array.downcast(coord_type="interleaved")
    assert point_arr == point_arr2


def test_downcast_with_crs():
    coords = np.array([[1, 4], [2, 5], [3, 6]], dtype=np.float64)

    crs = "EPSG:4326"
    point_arr = points(coords, crs=crs)
    geometry_array = point_arr.cast(geometry(crs=crs))
    point_arr2 = geometry_array.downcast(coord_type="interleaved")
    assert point_arr == point_arr2


class CustomException(Exception):
    pass


class ArrowCArrayFails:
    def __arrow_c_array__(self, requested_schema=None):
        raise CustomException


def test_array_import_preserve_exception():
    """https://github.com/kylebarron/arro3/issues/325"""

    c_stream_obj = ArrowCArrayFails()
    with pytest.raises(CustomException):
        GeoArray.from_arrow(c_stream_obj)
