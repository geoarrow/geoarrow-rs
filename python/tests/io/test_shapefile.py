import geodatasets
from geoarrow.rust.io import read_shapefile


def test_read_shapefile():
    shp_path = geodatasets.get_path("ny.bb")
    dbf_path = shp_path.rstrip(".shp") + ".dbf"

    table = read_shapefile(shp_path, dbf_path)
    assert len(table) == 5
