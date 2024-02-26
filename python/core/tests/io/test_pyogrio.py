import geoarrow.rust.core as gars
import geodatasets
import geopandas as gpd
import numpy as np

nybb_path = geodatasets.get_path("nybb")


def test_read_pyogrio():
    table = gars.read_pyogrio(nybb_path)
    gdf = gpd.read_file(nybb_path)
    assert np.array_equal(table.geometry.to_shapely(), gdf.geometry)
