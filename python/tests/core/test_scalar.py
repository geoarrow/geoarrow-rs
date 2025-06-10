import geopandas as gpd
import pyarrow as pa
import shapely
from arro3.core import Scalar
from geoarrow.rust.core import GeoArrowArray, point
from geoarrow.types.type_pyarrow import registered_extension_types


def test_eq():
    geoms = shapely.points([1, 2, 3], [4, 5, 6])
    arr = GeoArrowArray.from_arrow(gpd.GeoSeries(geoms).to_arrow("geoarrow"))
    assert arr[0] == arr[0]

    # pyarrow doesn't implement __arrow_c_array__ on scalars
    with registered_extension_types():
        pa_arr = pa.array(arr)
        assert arr[0] == GeoArrowArray.from_arrow(pa_arr)[0]

    # test with arro3
    assert arr[0] == Scalar.from_arrow(arr[0])
    assert arr[0] == GeoArrowArray.from_arrow(Scalar.from_arrow(arr[0]))[0]


def test_type():
    geoms = shapely.points([1, 2, 3], [4, 5, 6])
    arr = GeoArrowArray.from_arrow(gpd.GeoSeries(geoms).to_arrow("geoarrow"))
    assert arr[0].type == point("xy", coord_type="interleaved")
