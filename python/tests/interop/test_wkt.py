import pyarrow as pa
import shapely
from geoarrow.rust.core import from_wkt, to_shapely
from shapely.testing import assert_geometries_equal


def test_from_wkt():
    s = [
        "POINT (3 2)",
        "POINT (0 2)",
        "POINT (1 4)",
        "POINT (3 2)",
        "POINT (0 2)",
        "POINT (1 4)",
    ]
    shapely_arr = shapely.from_wkt(s)
    geo_arr = from_wkt(pa.array(s))
    assert_geometries_equal(shapely_arr, to_shapely(geo_arr))


def test_from_wkt_chunked():
    s1 = ["POINT (3 2)", "POINT (0 2)", "POINT (1 4)"]
    s2 = ["POINT (3 2)", "POINT (0 2)", "POINT (1 4)"]
    ca = pa.chunked_array([pa.array(s1), pa.array(s2)])
    shapely_arr = shapely.from_wkt(s1 + s2)
    geo_arr = from_wkt(ca)
    assert_geometries_equal(shapely_arr, to_shapely(geo_arr))
