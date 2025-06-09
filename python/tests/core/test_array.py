import geopandas as gpd
import pyarrow as pa
import shapely
from geoarrow.rust.core import GeoArrowArray
from geoarrow.types.type_pyarrow import registered_extension_types


def test_eq():
    geoms = shapely.points([1, 2, 3], [4, 5, 6])
    arr = GeoArrowArray.from_arrow(gpd.GeoSeries(geoms).to_arrow("geoarrow"))
    assert arr == arr

    with registered_extension_types():
        pa_arr = pa.array(arr)
        assert arr == pa_arr
        assert arr == GeoArrowArray.from_arrow(pa_arr)
