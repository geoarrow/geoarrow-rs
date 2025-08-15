from tempfile import TemporaryDirectory

import pyogrio
import pytest
from arro3.core import DataType
from geoarrow.rust.core import GeoType
from geoarrow.rust.io import read_flatgeobuf, read_flatgeobuf_async, write_flatgeobuf
from obstore.store import LocalStore

from tests.utils import FIXTURES_DIR


def test_read_flatgeobuf():
    path = FIXTURES_DIR / "flatgeobuf" / "countries.fgb"
    table = read_flatgeobuf(path)

    assert table.num_rows == 179
    assert table.num_columns == 3
    assert (
        table.schema.field("geometry").metadata_str["ARROW:extension:name"]
        == "geoarrow.multipolygon"
    )


def test_read_flatgeobuf_no_geometry():
    path = FIXTURES_DIR / "flatgeobuf" / "countries.fgb"
    table = read_flatgeobuf(path, read_geometry=False)
    assert table.column_names == ["id", "name"]


def test_read_flatgeobuf_columns():
    path = FIXTURES_DIR / "flatgeobuf" / "countries.fgb"
    table = read_flatgeobuf(path, columns=["id"])
    assert table.column_names == ["id", "geometry"]


def test_read_flatgeobuf_batch_size():
    path = FIXTURES_DIR / "flatgeobuf" / "countries.fgb"
    table = read_flatgeobuf(path, batch_size=5)
    assert table.to_batches()[0].num_rows == 5


def test_read_flatgeobuf_coord_type():
    path = FIXTURES_DIR / "flatgeobuf" / "countries.fgb"
    table = read_flatgeobuf(path, coord_type="interleaved")
    assert GeoType.from_arrow(table["geometry"].field).coord_type == "interleaved"


def test_read_flatgeobuf_view_types():
    path = FIXTURES_DIR / "flatgeobuf" / "countries.fgb"
    table = read_flatgeobuf(path, use_view_types=False)
    assert DataType.is_string(table["id"].type)


@pytest.mark.asyncio
async def test_read_flatgeobuf_async():
    store = LocalStore(FIXTURES_DIR)
    table = await read_flatgeobuf_async(
        "flatgeobuf/countries.fgb",
        store=store,
    )

    assert table.num_rows == 179
    assert table.num_columns == 3
    assert (
        table.schema.field("geometry").metadata_str["ARROW:extension:name"]
        == "geoarrow.multipolygon"
    )


def test_write_flatgeobuf_with_wkb_geometry():
    path = FIXTURES_DIR / "flatgeobuf" / "countries.fgb"
    # pyogrio.read_arrow leaves the geometry as WKB when reading.
    (meta, table) = pyogrio.read_arrow(path)

    field_meta = table.schema.field("wkb_geometry").metadata
    assert field_meta[b"ARROW:extension:name"] == b"geoarrow.wkb"

    with TemporaryDirectory() as tmpdir:
        tmp_path = f"{tmpdir}/countries.fgb"
        write_flatgeobuf(table, tmp_path, write_index=False)

        (meta2, table2) = pyogrio.read_arrow(path)

        assert table == table2

    with TemporaryDirectory() as tmpdir:
        tmp_path = f"{tmpdir}/countries.fgb"
        write_flatgeobuf(table, tmp_path, write_index=True)

        (meta2, table3) = pyogrio.read_arrow(path)

        assert table == table3
