import geodatasets
import geopandas as gpd
from geoarrow.rust.core import from_geopandas, geometry_col
from geoarrow.rust.core.enums import CoordType, Dimension

nybb_path = geodatasets.get_path("nybb")


def test_geometry_type():
    gdf = gpd.read_file(nybb_path)
    table = from_geopandas(gdf)
    geometry = geometry_col(table)
    geometry_type = geometry.type

    assert geometry_type.coord_type == CoordType.Interleaved
    assert geometry_type.dimension == Dimension.XY

    # TODO: come back to this
    # assert geometry_type == NativeType(
    #     "multipolygon",
    #     Dimension.XY,
    #     CoordType.Interleaved,
    # )
