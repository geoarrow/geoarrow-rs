import geodatasets
import geopandas as gpd
import pyarrow as pa
import shapely
from geoarrow.rust.core import GeoArray
from geoarrow.types.type_pyarrow import registered_extension_types


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
