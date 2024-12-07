from io import BytesIO

from geoarrow.rust.io import read_flatgeobuf, read_csv, write_csv

from tests.utils import FIXTURES_DIR


def test_read_write_csv():
    # Load data
    path = FIXTURES_DIR / "flatgeobuf" / "countries.fgb"
    table = read_flatgeobuf(path)

    # Write to csv
    buf = BytesIO()
    write_csv(table, buf)
    print(buf.getvalue().decode())

    # Read back from CSV
    buf.seek(0)
    retour = read_csv(buf)

    # The geometry type from the CSV is a generic geometry, so we can't do more
    # assertions until we have a geometry equality operation.
    assert len(table) == len(retour)
    assert table.schema.names == retour.schema.names
