from arro3.core import Field
import pyproj
import pytest

from geoarrow.rust.core import (
    GeoArrowType,
    point,
    linestring,
    polygon,
    multipoint,
    multilinestring,
    multipolygon,
    geometrycollection,
    geometry,
    box,
    wkb,
    wkt,
)
from geoarrow.rust.core.enums import CoordType, Dimension, Edges

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
        t = constructor(dim, coord_type)
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
    t = geometry(coord_type)
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
    assert Field.from_arrow(t).metadata_str[ARROW_EXTENSION_NAME] == "geoarrow.wkt"


def test_create_wkb():
    t = wkb()
    assert isinstance(t, GeoArrowType)
    assert t.dimension is None
    assert t.coord_type is None
    assert Field.from_arrow(t).metadata_str[ARROW_EXTENSION_NAME] == "geoarrow.wkb"


def test_crs():
    point_type = point(Dimension.XY, CoordType.INTERLEAVED, crs="EPSG:4326")
    # Note: this is pyproj magic to compare a string to a pyproj CRS object
    assert point_type.crs == "EPSG:4326"
    assert isinstance(point_type.crs, pyproj.CRS)


def test_edges():
    point_type = point(Dimension.XY, CoordType.INTERLEAVED, edges=Edges.VINCENTY)
    assert point_type.edges == Edges.VINCENTY
    assert isinstance(point_type.edges, Edges)
