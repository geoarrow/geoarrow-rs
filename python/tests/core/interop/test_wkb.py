import geodatasets
import geopandas as gpd
from arro3.core import ChunkedArray
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
    to_wkb,
    wkb,
)
from pyproj import CRS


def test_parse_points():
    geoms = shapely.points([1, 2], [4, 5])
    wkb_geoms = pa.array(shapely.to_wkb(geoms))

    parsed1 = from_wkb(wkb_geoms)
    assert parsed1.type == geometry()

    parsed2 = from_wkb(wkb_geoms, point("xy"))
    assert parsed2.type == point("xy")

    assert wkb_geoms == to_wkb(parsed1)
    assert wkb_geoms == to_wkb(parsed2)


def test_parse_nybb():
    gdf = gpd.read_file(geodatasets.get_path("ny.bb"))
    wkb_geoms = pa.array(gdf.geometry.to_wkb())
    parsed1 = from_wkb(wkb_geoms)
    assert parsed1.type == geometry()

    parsed2 = from_wkb(wkb_geoms, multipolygon("xy"))
    assert parsed2.type == multipolygon("xy")

    parsed3 = from_wkb(wkb_geoms, wkb())
    assert parsed3.type == wkb()

    parsed4 = from_wkb(wkb_geoms, wkb(crs="EPSG:4326"))
    assert get_crs(parsed4) == CRS("EPSG:4326")
    assert parsed4.type == wkb(crs="EPSG:4326")

    assert wkb_geoms == to_wkb(parsed1)
    assert wkb_geoms == to_wkb(parsed2)
    assert wkb_geoms == to_wkb(parsed3)


@pytest.mark.skip("debug why assert parsed1.type == geometry() is false")
def test_parse_nybb_chunked():
    gdf = gpd.read_file(geodatasets.get_path("ny.bb"))
    wkb_ca = ChunkedArray.from_arrow(gdf.geometry.to_arrow(geometry_encoding="wkb"))

    parsed1 = from_wkb(wkb_ca)
    assert isinstance(parsed1, GeoArrayReader)
    assert parsed1.type == geometry()

    # Note that this works to parse a MultiPolygon as a point because no parsing has
    # happened yet; it's lazy
    parsed2 = from_wkb(wkb_ca, point("xy"))
    with pytest.raises(Exception, match="Incorrect geometry type for operation"):
        parsed2.read_next_array()

    parsed3 = from_wkb(wkb_ca, multipolygon("xy"))
    assert isinstance(parsed3, GeoArrayReader)
    assert parsed3.type == multipolygon("xy")
    geo_chunked_array = parsed3.read_all()
    assert geo_chunked_array.type == multipolygon("xy")

    assert pa.chunked_array(wkb_ca) == pa.chunked_array(to_wkb(from_wkb(wkb_ca)))
    assert pa.chunked_array(wkb_ca) == pa.chunked_array(
        to_wkb(from_wkb(wkb_ca, multipolygon("xy")))
    )
