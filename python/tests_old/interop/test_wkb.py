import pyarrow as pa
import pytest
import shapely
from arro3.core import Array, DataType
from geoarrow.rust.core import from_shapely, from_wkb, to_shapely, to_wkb
from geoarrow.rust.core.enums import CoordType
from shapely.testing import assert_geometries_equal


@pytest.mark.skip
def test_wkb_round_trip():
    geoms = shapely.points([0, 1, 2, 3], [4, 5, 6, 7])
    geo_arr = from_shapely(geoms)
    wkb_arr = to_wkb(geo_arr)
    assert pa.array(shapely.to_wkb(geoms, flavor="iso")) == pa.array(wkb_arr)
    assert_geometries_equal(geoms, to_shapely(from_wkb(wkb_arr)))


@pytest.mark.skip
def test_geometry_collection():
    point = shapely.geometry.Point(0, 1)
    point2 = shapely.geometry.Point(1, 2)
    line_string = shapely.geometry.LineString([point, point2])
    gc = shapely.geometry.GeometryCollection([point, point2, line_string])
    wkb_arr = pa.array(shapely.to_wkb([gc]))

    parsed_geoarrow = from_wkb(wkb_arr)
    pa.array(parsed_geoarrow)
    retour = to_wkb(parsed_geoarrow)
    retour_shapely = shapely.from_wkb(retour[0].as_py())

    assert retour_shapely.geoms[0] == point
    assert retour_shapely.geoms[1] == point2
    assert retour_shapely.geoms[2] == line_string


@pytest.mark.skip
def test_ewkb_srid():
    geoms = shapely.points([0, 1, 2, 3], [4, 5, 6, 7])
    geoms = shapely.set_srid(geoms, 4326)
    assert shapely.get_srid(geoms)[0] == 4326

    ewkb_array = pa.array(shapely.to_wkb(geoms, flavor="extended", include_srid=True))
    retour = to_shapely(from_wkb(ewkb_array))
    assert all(geoms == retour)


@pytest.mark.skip
def test_from_wkb_coord_type():
    geoms = shapely.points([0, 1, 2, 3], [4, 5, 6, 7])

    geo_arr = from_wkb(
        Array(shapely.to_wkb(geoms), type=DataType.binary()),  # type: ignore
        coord_type=CoordType.Interleaved,
    )
    assert geo_arr.type.coord_type == CoordType.Interleaved

    geo_arr = from_wkb(
        Array(shapely.to_wkb(geoms), type=DataType.binary()),  # type: ignore
        coord_type=CoordType.Separated,
    )
    assert geo_arr.type.coord_type == CoordType.Separated
