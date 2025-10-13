import geodatasets
import geopandas as gpd
import numpy as np
import pyarrow as pa
import pytest
import shapely
from arro3.core import ChunkedArray
from geoarrow.rust.core import GeoArray, GeoChunkedArray, geometry, points
from geoarrow.types.type_pyarrow import registered_extension_types


def test_eq():
    geoms1 = shapely.points([1, 2, 3], [4, 5, 6])
    geoms2 = shapely.points([10, 20, 30], [40, 50, 60])
    arr1 = GeoArray.from_arrow(gpd.GeoSeries(geoms1).to_arrow("geoarrow"))
    arr2 = GeoArray.from_arrow(gpd.GeoSeries(geoms2).to_arrow("geoarrow"))
    ca = GeoChunkedArray.from_arrow(ChunkedArray([arr1, arr2]))

    assert ca == ca

    with registered_extension_types():
        pa_ca = pa.chunked_array(ca)
        assert ca == pa_ca
        assert ca == GeoChunkedArray.from_arrow(pa_ca)


def test_getitem():
    # Tests both the __getitem__ method and the scalar geo interface in round trip
    # conversion to shapely.
    gdf = gpd.read_file(geodatasets.get_path("ny.bb"))
    arr1 = GeoArray.from_arrow(gdf.geometry.iloc[:2].to_arrow("geoarrow"))
    arr2 = GeoArray.from_arrow(gdf.geometry.iloc[2:].to_arrow("geoarrow"))
    ca = GeoChunkedArray.from_arrow(ChunkedArray([arr1, arr2]))

    for i in range(len(ca)):
        assert shapely.geometry.shape(ca[i]).equals(gdf.geometry.iloc[i])  # type: ignore


def test_repr():
    geoms1 = shapely.points([1, 2, 3], [4, 5, 6])
    geoms2 = shapely.points([10, 20, 30], [40, 50, 60])
    arr1 = GeoArray.from_arrow(gpd.GeoSeries(geoms1).to_arrow("geoarrow"))
    arr2 = GeoArray.from_arrow(gpd.GeoSeries(geoms2).to_arrow("geoarrow"))
    ca = GeoChunkedArray.from_arrow(ChunkedArray([arr1, arr2]))
    assert (
        repr(ca) == 'GeoChunkedArray(Point(dimension="XY", coord_type="interleaved"))'
    )


def test_downcast():
    coords = np.array([[1, 4], [2, 5], [3, 6]], dtype=np.float64)

    point_arr = points(coords)
    point_ca = GeoChunkedArray.from_arrow(ChunkedArray([point_arr]))
    geometry_array = point_ca.cast(geometry())
    point_ca2 = geometry_array.downcast(coord_type="interleaved")
    assert point_ca == point_ca2


def test_downcast_with_crs():
    coords = np.array([[1, 4], [2, 5], [3, 6]], dtype=np.float64)

    crs = "EPSG:4326"
    point_arr = points(coords, crs=crs)
    point_ca = GeoChunkedArray.from_arrow(ChunkedArray([point_arr]))
    geometry_array = point_ca.cast(geometry(crs=crs))
    point_ca2 = geometry_array.downcast(coord_type="interleaved")
    assert point_ca == point_ca2


class CustomException(Exception):
    pass


class ArrowCStreamFails:
    def __arrow_c_stream__(self, requested_schema=None):
        raise CustomException


class ArrowCArrayFails:
    def __arrow_c_array__(self, requested_schema=None):
        raise CustomException


def test_chunked_array_import_preserve_exception():
    """https://github.com/kylebarron/arro3/issues/325"""

    c_stream_obj = ArrowCStreamFails()
    with pytest.raises(CustomException):
        GeoChunkedArray.from_arrow(c_stream_obj)

    with pytest.raises(CustomException):
        GeoChunkedArray(c_stream_obj)

    c_array_obj = ArrowCArrayFails()
    with pytest.raises(CustomException):
        GeoChunkedArray.from_arrow(c_array_obj)

    with pytest.raises(CustomException):
        GeoChunkedArray(c_array_obj)
