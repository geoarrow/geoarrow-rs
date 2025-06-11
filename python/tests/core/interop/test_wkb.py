import pyarrow as pa
import shapely
from geoarrow.rust.core import from_wkb, geometry, point


def test_parse_points():
    geoms = shapely.points([1, 2], [4, 5])
    wkb_geoms = pa.array(shapely.to_wkb(geoms))
    parsed = from_wkb(wkb_geoms)
    assert parsed.type == geometry()

    parsed = from_wkb(wkb_geoms, point("xy"))
    assert parsed.type == point("xy")
