import geodatasets
from geoarrow.rust.core import get_crs
from geoarrow.rust.io import read_shapefile
from pyproj import CRS


def test_read_shapefile():
    shp_path = geodatasets.get_path("ny.bb")

    table = read_shapefile(shp_path)
    crs = get_crs(table)
    assert crs is not None

    assert crs == CRS.from_epsg(2263)

    assert len(table) == 5
