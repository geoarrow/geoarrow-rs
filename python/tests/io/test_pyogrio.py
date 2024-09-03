import geoarrow.rust.core as gars
import geodatasets
import geopandas as gpd
import shapely.testing
from geoarrow.rust.core import to_shapely

nybb_path = geodatasets.get_path("nybb")


def test_read_pyogrio():
    table = gars.read_pyogrio(nybb_path)
    gdf = gpd.read_file(nybb_path)
    shapely.testing.assert_geometries_equal(
        to_shapely(gars.geometry_col(table)), gdf.geometry
    )
