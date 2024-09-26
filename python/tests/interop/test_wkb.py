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


def test_geometry_collection():
    point = shapely.geometry.Point(0, 1)
    point2 = shapely.geometry.Point(1, 2)
    line_string = shapely.geometry.LineString([point, point2])
    gc = shapely.geometry.GeometryCollection([point, point2, line_string])
    wkb_arr = pa.array(shapely.to_wkb([gc]))

    parsed_geoarrow = from_wkb(wkb_arr)
    pa.array(parsed_geoarrow)
    retour = to_wkb(parsed_geoarrow)
    retour_shapely = shapely.from_wkb(retour[0].as_py())

    # Need to unpack the geoms because they're returned as multi-geoms
    assert retour_shapely.geoms[0].geoms[0] == point
    assert retour_shapely.geoms[1].geoms[0] == point2
    assert retour_shapely.geoms[2].geoms[0] == line_string
