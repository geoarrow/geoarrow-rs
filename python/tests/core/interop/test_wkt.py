import geodatasets
import geopandas as gpd
import pyarrow as pa
import pytest
import shapely
import shapely.testing
from geoarrow.rust.core import (
    GeoArrayReader,
    from_wkt,
    geometry,
    get_crs,
    multipolygon,
    point,
    to_wkt,
    wkt,
)
from pyproj import CRS


def test_parse_points():
    geoms = shapely.points([1, 2], [4, 5])
    wkt_geoms = pa.array(shapely.to_wkt(geoms))

    parsed1 = from_wkt(wkt_geoms)
    assert parsed1.type == geometry()

    parsed2 = from_wkt(wkt_geoms, point("xy"))
    assert parsed2.type == point("xy")

    assert remove_initial_whitespace(wkt_geoms) == pa.array(to_wkt(parsed1))
    assert remove_initial_whitespace(wkt_geoms) == to_wkt(parsed2)


def test_parse_nybb():
    gdf = gpd.read_file(geodatasets.get_path("ny.bb"))
    wkt_geoms = pa.array(shapely.to_wkt(gdf.geometry, rounding_precision=15))
    # wkt_geoms = pa.array(gdf.geometry.to_wkt())
    parsed1 = from_wkt(wkt_geoms)
    assert parsed1.type == geometry()

    parsed2 = from_wkt(wkt_geoms, multipolygon("xy"))
    assert parsed2.type == multipolygon("xy")

    parsed3 = from_wkt(wkt_geoms, wkt())
    assert parsed3.type == wkt()

    parsed4 = from_wkt(wkt_geoms, wkt(crs="EPSG:4326"))
    assert get_crs(parsed4) == CRS("EPSG:4326")
    assert parsed4.type == wkt(crs="EPSG:4326")

    for i in range(len(gdf.geometry)):
        assert_shapely_equal_via_wkt(geom=gdf.geometry[i])


def test_parse_nybb_chunked():
    gdf = gpd.read_file(geodatasets.get_path("ny.bb"))
    wkt_geoms = pa.array(gdf.geometry.to_wkt())
    wkt_ca = pa.chunked_array([wkt_geoms, wkt_geoms])

    parsed1 = from_wkt(wkt_ca)
    assert isinstance(parsed1, GeoArrayReader)
    assert parsed1.type == geometry()

    # Note that this works to parse a MultiPolygon as a point because no parsing has
    # happened yet; it's lazy
    parsed2 = from_wkt(wkt_ca, point("xy"))
    with pytest.raises(Exception, match="Incorrect geometry type for operation"):
        parsed2.read_next_array()

    parsed3 = from_wkt(wkt_ca, multipolygon("xy"))
    assert isinstance(parsed3, GeoArrayReader)
    assert parsed3.type == multipolygon("xy")
    geo_chunked_array = parsed3.read_all()
    assert geo_chunked_array.type == multipolygon("xy")

    assert wkt_ca == to_wkt(from_wkt(wkt_ca)).read_all()
    assert wkt_ca == to_wkt(from_wkt(wkt_ca, multipolygon("xy"))).read_all()


def remove_initial_whitespace(arr: pa.Array) -> pa.Array:
    """Remove initial whitespace from each string in the array."""
    return pa.array(["".join(s.as_py().split(" ", 1)) for s in arr])


def assert_shapely_equal_via_wkt(geom: shapely.geometry.base.BaseGeometry):
    wkt = pa.array([shapely.to_wkt(geom, rounding_precision=15)])
    back_array = pa.array(to_wkt(from_wkt(wkt)))
    back_geom = shapely.from_wkt(back_array[0].as_py())
    shapely.testing.assert_geometries_equal(geom, back_geom)
