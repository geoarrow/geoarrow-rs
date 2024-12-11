from io import BytesIO

from geoarrow.rust.core import geometry_col
from geoarrow.rust.io import read_flatgeobuf, read_csv, write_csv
from arro3.core import DataType

from tests.utils import FIXTURES_DIR


def test_read_write_csv():
    # Load data
    path = FIXTURES_DIR / "flatgeobuf" / "countries.fgb"
    table = read_flatgeobuf(path)

    # Write to csv
    buf = BytesIO()
    write_csv(table, buf)
    # print(buf.getvalue().decode())

    # Read back from CSV
    buf.seek(0)
    retour = read_csv(buf)

    # The geometry type from the CSV is a generic geometry, so we can't do more
    # assertions until we have a geometry equality operation.
    assert len(table) == len(retour)
    assert table.schema.names == retour.schema.names
    assert geometry_col(table).type == geometry_col(retour).type


CSV_TEXT = """
address,type,datetime,report location,incident number
904 7th Av,Car Fire,05/22/2019 12:55:00 PM,POINT (-122.329051 47.6069),F190051945
9610 53rd Av S,Aid Response,05/22/2019 12:55:00 PM,POINT (-122.266529 47.515984),F190051946"
"""


def test_downcast():
    table = read_csv(BytesIO(CSV_TEXT.encode()), geometry_name="report location")
    assert DataType.is_fixed_size_list(table["geometry"].type)


def test_reader_no_downcast():
    reader = read_csv(
        BytesIO(CSV_TEXT.encode()),
        geometry_name="report location",
        downcast_geometry=False,
    )
    table = reader.read_all()
    assert table.num_rows == 2
