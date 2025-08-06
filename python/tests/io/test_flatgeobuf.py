from tempfile import TemporaryDirectory

import pyogrio
from geoarrow.rust.io import read_flatgeobuf
# from geoarrow.rust.io import write_flatgeobuf

from tests.utils import FIXTURES_DIR


def test_read_flatgeobuf():
    path = FIXTURES_DIR / "flatgeobuf" / "countries.fgb"
    table = read_flatgeobuf(path)
    pyogrio.read_arrow(path)

    assert table.num_rows == 179
    assert table.num_columns == 3
    assert (
        table.schema.field("geometry").metadata_str["ARROW:extension:name"]
        == "geoarrow.multipolygon"
    )


# def test_write_flatgeobuf_with_wkb_geometry():
#     path = FIXTURES_DIR / "flatgeobuf" / "countries.fgb"
#     # pyogrio.read_arrow leaves the geometry as WKB when reading.
#     (meta, table) = pyogrio.read_arrow(path)

#     field_meta = table.schema.field("wkb_geometry").metadata
#     assert field_meta[b"ARROW:extension:name"] == b"geoarrow.wkb"

#     with TemporaryDirectory() as tmpdir:
#         tmp_path = f"{tmpdir}/countries.fgb"
#         write_flatgeobuf(table, tmp_path, write_index=False)

#         (meta2, table2) = pyogrio.read_arrow(path)

#     assert table == table2
