import pyarrow as pa
import shapely
from geoarrow.rust.core import from_shapely, from_wkb, to_shapely, to_wkb
from shapely.testing import assert_geometries_equal


def test_wkb_round_trip():
    geoms = shapely.points([0, 1, 2, 3], [4, 5, 6, 7])
    geo_arr = from_shapely(geoms)
    wkb_arr = to_wkb(geo_arr)
    assert pa.array(shapely.to_wkb(geoms, flavor="iso")) == pa.array(wkb_arr)
    assert_geometries_equal(geoms, to_shapely(from_wkb(wkb_arr)))
