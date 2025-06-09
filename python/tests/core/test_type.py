import pyarrow as pa
import pyproj
import pytest
from arro3.core import DataType, Field
from geoarrow.rust.core import (
    GeoArrowType,
    box,
    geometry,
    geometrycollection,
    large_wkb,
    large_wkt,
    linestring,
    multilinestring,
    multipoint,
    multipolygon,
    point,
    polygon,
    wkb,
    wkb_view,
    wkt,
    wkt_view,
)
from geoarrow.rust.core.enums import CoordType, Dimension, Edges
from geoarrow.types.type_pyarrow import PointType, registered_extension_types

ARROW_EXTENSION_NAME = "ARROW:extension:name"


@pytest.mark.parametrize(
    "dim,coord_type",
    [
        (Dimension.XY, CoordType.INTERLEAVED),
        (Dimension.XY, CoordType.SEPARATED),
        (Dimension.XYZ, CoordType.INTERLEAVED),
        (Dimension.XYZ, CoordType.SEPARATED),
        (Dimension.XYM, CoordType.INTERLEAVED),
        (Dimension.XYM, CoordType.SEPARATED),
        (Dimension.XYZM, CoordType.INTERLEAVED),
        (Dimension.XYZM, CoordType.SEPARATED),
    ],
)
def test_create_native_type(dim: Dimension, coord_type: CoordType):
    for constructor in [
        point,
        linestring,
        polygon,
        multipoint,
        multilinestring,
        multipolygon,
        geometrycollection,
    ]:
        t = constructor(dim, coord_type=coord_type)
        assert isinstance(t, GeoArrowType)
        assert t.dimension == dim
        assert t.coord_type == coord_type
        assert (
            Field.from_arrow(t).metadata_str[ARROW_EXTENSION_NAME]
            == f"geoarrow.{constructor.__name__}"
        )
        assert isinstance(t.dimension, Dimension)
        assert isinstance(t.coord_type, CoordType)


@pytest.mark.parametrize("coord_type", [CoordType.INTERLEAVED, CoordType.SEPARATED])
def test_create_geometry(coord_type: CoordType):
    t = geometry(coord_type=coord_type)
    assert isinstance(t, GeoArrowType)
    assert t.dimension is None
    assert t.coord_type == coord_type
    assert Field.from_arrow(t).metadata_str[ARROW_EXTENSION_NAME] == "geoarrow.geometry"


@pytest.mark.parametrize(
    "dim", [Dimension.XY, Dimension.XYZ, Dimension.XYM, Dimension.XYZM]
)
def test_create_box(dim: Dimension):
    t = box(dim)
    assert isinstance(t, GeoArrowType)
    assert t.dimension == dim
    assert t.coord_type == CoordType.SEPARATED
    assert Field.from_arrow(t).metadata_str[ARROW_EXTENSION_NAME] == "geoarrow.box"


def test_create_wkt():
    t = wkt()
    assert isinstance(t, GeoArrowType)
    assert t.dimension is None
    assert t.coord_type is None
    f = Field.from_arrow(t)
    assert f.metadata_str[ARROW_EXTENSION_NAME] == "geoarrow.wkt"
    assert f.type == DataType.string()


def test_create_large_wkt():
    t = large_wkt()
    assert isinstance(t, GeoArrowType)
    assert t.dimension is None
    assert t.coord_type is None
    f = Field.from_arrow(t)
    assert f.metadata_str[ARROW_EXTENSION_NAME] == "geoarrow.wkt"
    assert f.type == DataType.large_string()


def test_create_wkt_view():
    t = wkt_view()
    assert isinstance(t, GeoArrowType)
    assert t.dimension is None
    assert t.coord_type is None
    f = Field.from_arrow(t)
    assert f.metadata_str[ARROW_EXTENSION_NAME] == "geoarrow.wkt"
    assert f.type == DataType.string_view()


def test_create_wkb():
    t = wkb()
    assert isinstance(t, GeoArrowType)
    assert t.dimension is None
    assert t.coord_type is None
    f = Field.from_arrow(t)
    assert f.metadata_str[ARROW_EXTENSION_NAME] == "geoarrow.wkb"
    assert f.type == DataType.binary()


def test_create_large_wkb():
    t = large_wkb()
    assert isinstance(t, GeoArrowType)
    assert t.dimension is None
    assert t.coord_type is None
    f = Field.from_arrow(t)
    assert f.metadata_str[ARROW_EXTENSION_NAME] == "geoarrow.wkb"
    assert f.type == DataType.large_binary()


def test_create_wkb_view():
    t = wkb_view()
    assert isinstance(t, GeoArrowType)
    assert t.dimension is None
    assert t.coord_type is None
    f = Field.from_arrow(t)
    assert f.metadata_str[ARROW_EXTENSION_NAME] == "geoarrow.wkb"
    assert f.type == DataType.binary_view()


def test_crs():
    point_type = point("xy", crs="EPSG:4326")
    # Note: this is pyproj magic to compare a string to a pyproj CRS object
    assert point_type.crs == "EPSG:4326"
    assert isinstance(point_type.crs, pyproj.CRS)


def test_edges():
    point_type = point(Dimension.XY, edges=Edges.VINCENTY)
    assert point_type.edges == Edges.VINCENTY
    assert isinstance(point_type.edges, Edges)


def test_from_arrow():
    ga_type = point("xy")
    arro3_field = Field.from_arrow(ga_type)
    assert ga_type == GeoArrowType.from_arrow(arro3_field)


def test_from_geoarrow_pyarrow():
    rust_type = point("xy")
    with registered_extension_types():
        field = pa.field(rust_type)
        typ = field.type
        assert isinstance(typ, PointType)

        assert rust_type == GeoArrowType.from_arrow(typ)


def test_with_crs():
    point_type = point("xy")
    assert point_type.crs is None
    new_crs = pyproj.CRS.from_epsg(4326)
    updated_type = point_type.with_crs(new_crs, edges="spherical")
    assert updated_type.crs == new_crs
    assert updated_type.dimension == point_type.dimension
    assert updated_type.coord_type == point_type.coord_type
    assert updated_type.edges == Edges.SPHERICAL
