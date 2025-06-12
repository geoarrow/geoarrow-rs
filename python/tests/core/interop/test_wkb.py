import geodatasets
import geopandas as gpd
import pyarrow as pa
import pytest
import shapely
from geoarrow.rust.core import (
    GeoArrayReader,
    from_wkb,
    geometry,
    get_crs,
    multipolygon,
    point,
    wkb,
)
from pyproj import CRS


def test_parse_points():
    geoms = shapely.points([1, 2], [4, 5])
    wkb_geoms = pa.array(shapely.to_wkb(geoms))
    parsed = from_wkb(wkb_geoms)
    assert parsed.type == geometry()

    parsed = from_wkb(wkb_geoms, point("xy"))
    assert parsed.type == point("xy")


def test_parse_nybb():
    gdf = gpd.read_file(geodatasets.get_path("ny.bb"))
    wkb_geoms = pa.array(gdf.geometry.to_wkb())
    parsed = from_wkb(wkb_geoms)
    assert parsed.type == geometry()

    parsed = from_wkb(wkb_geoms, multipolygon("xy"))
    assert parsed.type == multipolygon("xy")

    parsed = from_wkb(wkb_geoms, wkb())
    assert parsed.type == wkb()

    parsed = from_wkb(wkb_geoms, wkb(crs="EPSG:4326"))
    assert get_crs(parsed) == CRS("EPSG:4326")
    assert parsed.type == wkb(crs="EPSG:4326")


def test_parse_nybb_chunked():
    gdf = gpd.read_file(geodatasets.get_path("ny.bb"))
    wkb_geoms = pa.array(gdf.geometry.to_wkb())
    wkb_ca = pa.chunked_array([wkb_geoms, wkb_geoms])
    parsed = from_wkb(wkb_ca)
    assert isinstance(parsed, GeoArrayReader)
    assert parsed.type == geometry()

    # Note that this works to parse a MultiPolygon as a point because no parsing has
    # happened yet; it's lazy
    parsed = from_wkb(wkb_ca, point("xy"))
    with pytest.raises(Exception, match="Incorrect geometry type for operation"):
        parsed.read_next_array()

    parsed = from_wkb(wkb_ca, multipolygon("xy"))
    assert isinstance(parsed, GeoArrayReader)
    assert parsed.type == multipolygon("xy")
    geo_chunked_array = parsed.read_all()
    assert geo_chunked_array.type == multipolygon("xy")
