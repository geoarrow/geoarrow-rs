import geodatasets
import geopandas as gpd
import shapely
import shapely.testing
from geoarrow.rust.core import from_geopandas, geometry_col

nybb_path = geodatasets.get_path("nybb")


def test_indexing():
    gdf = gpd.read_file(nybb_path)
    table = from_geopandas(gdf)
    test = geometry_col(table)
    scalar = test[0]
    shapely_scalar = shapely.geometry.shape(scalar)
    assert gdf.geometry[0] == shapely_scalar
