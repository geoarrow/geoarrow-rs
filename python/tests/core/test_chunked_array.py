import geodatasets
import geopandas as gpd
import pyarrow as pa
import shapely
from arro3.core import ChunkedArray
from geoarrow.rust.core import ChunkedGeoArrowArray, GeoArrowArray
from geoarrow.types.type_pyarrow import registered_extension_types


def test_eq():
    geoms1 = shapely.points([1, 2, 3], [4, 5, 6])
    geoms2 = shapely.points([10, 20, 30], [40, 50, 60])
    arr1 = GeoArrowArray.from_arrow(gpd.GeoSeries(geoms1).to_arrow("geoarrow"))
    arr2 = GeoArrowArray.from_arrow(gpd.GeoSeries(geoms2).to_arrow("geoarrow"))
    ca = ChunkedGeoArrowArray.from_arrow(ChunkedArray([arr1, arr2]))

    assert ca == ca

    with registered_extension_types():
        pa_ca = pa.chunked_array(ca)
        assert ca == pa_ca
        assert ca == ChunkedGeoArrowArray.from_arrow(pa_ca)


def test_getitem():
    # Tests both the __getitem__ method and the scalar geo interface in round trip
    # conversion to shapely.
    gdf = gpd.read_file(geodatasets.get_path("ny.bb"))
    arr1 = GeoArrowArray.from_arrow(gdf.geometry.iloc[:2].to_arrow("geoarrow"))
    arr2 = GeoArrowArray.from_arrow(gdf.geometry.iloc[2:].to_arrow("geoarrow"))
    ca = ChunkedGeoArrowArray.from_arrow(ChunkedArray([arr1, arr2]))

    for i in range(len(ca)):
        assert shapely.geometry.shape(ca[i]).equals(gdf.geometry.iloc[i])  # type: ignore
