import geodatasets
import geopandas as gpd
import shapely
import shapely.testing
from geoarrow.rust.core import from_geopandas, geometry_col

nybb_path = geodatasets.get_path("nybb")


def test_indexing():
    gdf = gpd.read_file(nybb_path)
    table = from_geopandas(gdf)
    geometry = geometry_col(table)

    shapely_scalar = shapely.geometry.shape(geometry[0])
    assert gdf.geometry[0] == shapely_scalar

    shapely_scalar = shapely.geometry.shape(geometry[-1])
    assert gdf.geometry.iloc[-1] == shapely_scalar
