from io import BytesIO

import geodatasets
import geopandas as gpd
import pyarrow as pa
import shapely
from geoarrow.rust.core import from_geopandas, geometry_col, to_geopandas, get_crs
from geoarrow.rust.io import read_flatgeobuf, write_flatgeobuf
from geopandas.testing import assert_geodataframe_equal

from tests_old.utils import FIXTURES_DIR


def test_read_flatgeobuf():
    path = FIXTURES_DIR / "flatgeobuf" / "countries.fgb"
    table = read_flatgeobuf(path)
    assert len(table) == 179
    # hacky
    assert "MultiPolygon" in geometry_col(table).type.__repr__()


def test_read_flatgeobuf_file_object():
    path = FIXTURES_DIR / "flatgeobuf" / "countries.fgb"
    with open(path, "rb") as f:
        table = read_flatgeobuf(f)
    assert len(table) == 179
    # hacky
    assert "MultiPolygon" in geometry_col(table).type.__repr__()


def test_round_trip_flatgeobuf():
    path = FIXTURES_DIR / "flatgeobuf" / "countries.fgb"
    table = read_flatgeobuf(path)

    buf = BytesIO()
    write_flatgeobuf(table, buf, write_index=False)
    buf.seek(0)
    table_back = read_flatgeobuf(buf)

    assert get_crs(table) == get_crs(table_back)

    # Table equality currently fails because of differences in exact CRS representation
    # assert table == table_back  # type: ignore


def test_round_trip_polygon():
    geom = shapely.geometry.shape(
        {
            "type": "Polygon",
            "coordinates": [
                [
                    [-118.4765625, 33.92578125],
                    [-118.125, 33.92578125],
                    [-118.125, 34.1015625],
                    [-118.4765625, 34.1015625],
                    [-118.4765625, 33.92578125],
                ],
                [
                    [-118.24447631835938, 34.0521240234375],
                    [-118.24310302734375, 34.0521240234375],
                    [-118.24310302734375, 34.053497314453125],
                    [-118.24447631835938, 34.053497314453125],
                    [-118.24447631835938, 34.0521240234375],
                ],
            ],
        }
    )
    polys = [geom] * 3
    gdf = gpd.GeoDataFrame({"col1": ["a", "b", "c"]}, geometry=polys, crs="EPSG:4326")
    table = from_geopandas(gdf)

    buf = BytesIO()
    write_flatgeobuf(table, buf, write_index=False)
    buf.seek(0)
    table_back = read_flatgeobuf(buf)
    assert pa.table(table) == pa.table(table_back)


def test_round_trip_3d_points():
    points = shapely.points([1, 2, 3], [4, 5, 6], [7, 8, 9])
    gdf = gpd.GeoDataFrame({"col1": ["a", "b", "c"]}, geometry=points, crs="EPSG:4326")
    table = from_geopandas(gdf)

    buf = BytesIO()
    write_flatgeobuf(table, buf, write_index=False)
    buf.seek(0)
    table_back = read_flatgeobuf(buf)

    assert pa.table(table) == pa.table(table_back)


def test_round_trip_multilinestring():
    gdf = gpd.read_file(geodatasets.get_path("eea.large_rivers"))
    table = from_geopandas(gdf)

    buf = BytesIO()
    write_flatgeobuf(table, buf, write_index=False)
    buf.seek(0)
    table_back = read_flatgeobuf(buf)

    assert pa.table(table) == pa.table(table_back)


def test_matches_pyogrio():
    path = FIXTURES_DIR / "flatgeobuf" / "countries.fgb"
    table = read_flatgeobuf(path)

    gdf_direct = gpd.read_file(path)
    gdf_from_rust = to_geopandas(table)

    # The geometry for antarctica is not valid. Which means that `shapely.equals` fails,
    # even though all the coordinates are the same.
    exclude_antarctica1 = gdf_direct[gdf_direct["name"] != "Antarctica"]
    exclude_antarctica2 = gdf_from_rust[gdf_direct["name"] != "Antarctica"]

    assert_geodataframe_equal(exclude_antarctica1, exclude_antarctica2)
