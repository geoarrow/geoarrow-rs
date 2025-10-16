import geodatasets
import geopandas as gpd
import pyarrow as pa
import shapely
from arro3.core import Scalar
from geoarrow.rust.core import GeoArray, point
from geoarrow.types.type_pyarrow import registered_extension_types

from tests.utils import geo_interface_equals


def test_eq():
    geoms = shapely.points([1, 2, 3], [4, 5, 6])
    arr = GeoArray.from_arrow(gpd.GeoSeries(geoms).to_arrow("geoarrow"))
    assert arr[0] == arr[0]

    # pyarrow doesn't implement __arrow_c_array__ on scalars
    with registered_extension_types():
        pa_arr = pa.array(arr)
        assert arr[0] == GeoArray.from_arrow(pa_arr)[0]

    # test with arro3
    assert arr[0] == Scalar.from_arrow(arr[0])
    assert arr[0] == GeoArray.from_arrow(Scalar.from_arrow(arr[0]))[0]


def test_type():
    geoms = shapely.points([1, 2, 3], [4, 5, 6])
    arr = GeoArray.from_arrow(gpd.GeoSeries(geoms).to_arrow("geoarrow"))
    assert arr[0].type == point("xy", coord_type="interleaved")


def test_repr():
    geoms = shapely.points([1, 2, 3], [4, 5, 6])
    arr = GeoArray.from_arrow(gpd.GeoSeries(geoms).to_arrow("geoarrow"))
    assert repr(arr[0]) == 'GeoScalar(Point(dimension="XY", coord_type="interleaved"))'


def test_geo_interface():
    geoms = shapely.points([1, 2, 3], [4, 5, 6])
    gs = gpd.GeoSeries(geoms)
    arr = GeoArray.from_arrow(gs.to_arrow("geoarrow"))
    assert geo_interface_equals(gs[0].__geo_interface__, arr[0].__geo_interface__)


def test_geo_interface_polygon():
    gdf = gpd.read_file(geodatasets.get_path("ny.bb"))
    arr = GeoArray.from_arrow(gdf.geometry.to_arrow("geoarrow"))
    assert geo_interface_equals(
        gdf.geometry.iloc[0].__geo_interface__,
        arr[0].__geo_interface__,
    )
