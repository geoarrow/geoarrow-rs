from geoarrow.rust.core.enums import CoordType, Dimension
import geodatasets
from geoarrow.rust.core import GeometryType
import geopandas as gpd
from geoarrow.rust.core import from_geopandas, geometry_col

nybb_path = geodatasets.get_path("nybb")


def test_geometry_type():
    gdf = gpd.read_file(nybb_path)
    table = from_geopandas(gdf)
    geometry = geometry_col(table)
    geometry_type = geometry.type

    assert geometry_type.coord_type == CoordType.Interleaved
    assert geometry_type.dimension == Dimension.XY

    assert geometry_type == GeometryType(
        "multipolygon",
        Dimension.XY,
        CoordType.Interleaved,
    )
