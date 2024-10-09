from io import BytesIO
import pyarrow as pa

import geopandas as gpd
import pytest
import shapely
from geoarrow.rust.core import to_geopandas, from_geopandas
from geoarrow.rust.io import read_flatgeobuf, write_flatgeobuf
from geopandas.testing import assert_geodataframe_equal

from tests.utils import FIXTURES_DIR


def test_read_flatgeobuf():
    path = FIXTURES_DIR / "flatgeobuf" / "countries.fgb"
    table = read_flatgeobuf(path)
    assert len(table) == 179
    # assert isinstance(gars.geometry_col(table), gars.ChunkedMultiPolygonArray)


def test_read_flatgeobuf_file_object():
    path = FIXTURES_DIR / "flatgeobuf" / "countries.fgb"
    with open(path, "rb") as f:
        table = read_flatgeobuf(f)
    assert len(table) == 179
    # assert isinstance(gars.geometry_col(table), gars.ChunkedMultiPolygonArray)


def test_round_trip_flatgeobuf():
    path = FIXTURES_DIR / "flatgeobuf" / "countries.fgb"
    table = read_flatgeobuf(path)

    buf = BytesIO()
    write_flatgeobuf(table, buf)
    buf.seek(0)
    table_back = read_flatgeobuf(buf)
    assert table == table_back  # type: ignore


def test_round_trip_3d():
    points = shapely.points([1, 2, 3], [4, 5, 6], [7, 8, 9])
    gdf = gpd.GeoDataFrame({"col1": ["a", "b", "c"]}, geometry=points, crs="EPSG:4326")
    table = from_geopandas(gdf)

    buf = BytesIO()
    write_flatgeobuf(table, buf, write_index=False)
    buf.seek(0)
    table_back = read_flatgeobuf(buf)

    assert pa.table(table) == pa.table(table_back)


@pytest.mark.xfail(reason="fix propagate CRS")
def test_matches_pyogrio():
    path = FIXTURES_DIR / "flatgeobuf" / "countries.fgb"
    table = read_flatgeobuf(path)

    gdf_direct = gpd.read_file(path)
    gdf_from_rust = to_geopandas(table)
    assert_geodataframe_equal(gdf_direct, gdf_from_rust)
