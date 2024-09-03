import geoarrow.rust.core as gars
import geodatasets
import geopandas as gpd
import numpy as np
import pyarrow as pa

nybb_path = geodatasets.get_path("nybb")


def test_area():
    gdf = gpd.read_file(nybb_path)
    shapely_area = gdf.geometry.area
    assert isinstance(gdf, gpd.GeoDataFrame)

    table = gars.from_geopandas(gdf)
    ga_area = gars.area(gars.geometry_col(table))
    pa_arr = pa.chunked_array(ga_area)
    assert pa_arr.num_chunks == 1

    pa_area = pa_arr.chunk(0)
    assert np.allclose(shapely_area, pa_area)
