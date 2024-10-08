import geodatasets
import geopandas as gpd
import pytest
import shapely.testing
from geoarrow.rust.core import geometry_col, read_pyogrio, to_shapely

nybb_path = geodatasets.get_path("nybb")


def test_read_pyogrio():
    _table = read_pyogrio(nybb_path)
    # gdf = gpd.read_file(nybb_path)
    # shapely.testing.assert_geometries_equal(
    #     to_shapely(geometry_col(table)), gdf.geometry
    # )


@pytest.mark.xfail(
    reason=(
        "to_shapely currently failing on WKB input with:\n"
        "Exception: General error: Unexpected extension name geoarrow.wkb"
    )
)
def test_read_pyogrio_round_trip():
    table = read_pyogrio(nybb_path)
    gdf = gpd.read_file(nybb_path)
    shapely.testing.assert_geometries_equal(
        to_shapely(geometry_col(table)), gdf.geometry
    )
