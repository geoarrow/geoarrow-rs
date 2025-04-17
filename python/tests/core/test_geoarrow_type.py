from arro3.core import Field
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
from geoarrow.rust.core.enums import CoordType, Dimension

ARROW_EXTENSION_NAME = "ARROW:extension:name"


@pytest.mark.parametrize(
    "dim,coord_type",
    [
        (Dimension.XY, CoordType.Interleaved),
        (Dimension.XY, CoordType.Separated),
        (Dimension.XYZ, CoordType.Interleaved),
        (Dimension.XYZ, CoordType.Separated),
        (Dimension.XYM, CoordType.Interleaved),
        (Dimension.XYM, CoordType.Separated),
        (Dimension.XYZM, CoordType.Interleaved),
        (Dimension.XYZM, CoordType.Separated),
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


@pytest.mark.parametrize("coord_type", [CoordType.Interleaved, CoordType.Separated])
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
    assert t.coord_type == CoordType.Separated
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
