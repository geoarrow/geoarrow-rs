from io import BytesIO

import geoarrow.rust.core as gars
import geopandas as gpd
import pytest
from geopandas.testing import assert_geodataframe_equal

from tests.utils import FIXTURES_DIR


def test_read_flatgeobuf():
    path = FIXTURES_DIR / "flatgeobuf" / "countries.fgb"
    table = gars.read_flatgeobuf(path)
    assert len(table) == 179
    # assert isinstance(gars.geometry_col(table), gars.ChunkedMultiPolygonArray)


def test_read_flatgeobuf_file_object():
    path = FIXTURES_DIR / "flatgeobuf" / "countries.fgb"
    with open(path, "rb") as f:
        table = gars.read_flatgeobuf(f)
    assert len(table) == 179
    # assert isinstance(gars.geometry_col(table), gars.ChunkedMultiPolygonArray)


def test_round_trip_flatgeobuf():
    path = FIXTURES_DIR / "flatgeobuf" / "countries.fgb"
    table = gars.read_flatgeobuf(path)

    buf = BytesIO()
    gars.write_flatgeobuf(table, buf)
    buf.seek(0)
    table_back = gars.read_flatgeobuf(buf)
    assert table == table_back  # type: ignore


@pytest.mark.xfail(reason="fix propagate CRS")
def test_matches_pyogrio():
    path = FIXTURES_DIR / "flatgeobuf" / "countries.fgb"
    table = gars.read_flatgeobuf(path)

    gdf_direct = gpd.read_file(path)
    gdf_from_rust = gars.to_geopandas(table)
    assert_geodataframe_equal(gdf_direct, gdf_from_rust)
