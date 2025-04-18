from arro3.core.types import ArrowSchemaExportable
from geoarrow.rust.core.enums import CoordType, Dimension, Edges
from geoarrow.rust.core.types import (
    CRSInput,
    CoordTypeInput,
    DimensionInput,
    EdgesInput,
)
from pyproj.crs.crs import CRS

__all__ = [
    "GeoArrowType",
    "point",
    "linestring",
    "polygon",
    "multipoint",
    "multilinestring",
    "multipolygon",
    "geometrycollection",
    "geometry",
    "box",
    "wkb",
    "wkt",
]

class GeoArrowType:
    """


    GeoArrowType is a wrapper around the GeoArrowType C++ class.
    """

    def __init__(self, data: object) -> None: ...
    def __arrow_c_schema__(self) -> object: ...
    def __eq__(self, other: object) -> bool: ...
    def __repr__(self) -> str: ...
    @classmethod
    def from_arrow(cls, data: ArrowSchemaExportable) -> GeoArrowType: ...
    @classmethod
    def from_arrow_pycapsule(cls, capsule: object) -> GeoArrowType: ...
    @property
    def coord_type(self) -> CoordType: ...
    @property
    def dimension(self) -> Dimension:
        """The dimension of the type."""
    @property
    def crs(self) -> CRS | None:
        """The CRS of the type."""
    @property
    def edges(self) -> Edges | None:
        """The edge interpretation of this type."""

def point(
    dimension: DimensionInput,
    coord_type: CoordTypeInput,
    *,
    crs: CRSInput | None = None,
    edges: EdgesInput | None = None,
) -> GeoArrowType:
    """Create a new Arrow type for a GeoArrow Point array."""

def linestring(
    dimension: DimensionInput,
    coord_type: CoordTypeInput,
    *,
    crs: CRSInput | None = None,
    edges: EdgesInput | None = None,
) -> GeoArrowType: ...
def polygon(
    dimension: DimensionInput,
    coord_type: CoordTypeInput,
    *,
    crs: CRSInput | None = None,
    edges: EdgesInput | None = None,
) -> GeoArrowType: ...
def multipoint(
    dimension: DimensionInput,
    coord_type: CoordTypeInput,
    *,
    crs: CRSInput | None = None,
    edges: EdgesInput | None = None,
) -> GeoArrowType: ...
def multilinestring(
    dimension: DimensionInput,
    coord_type: CoordTypeInput,
    *,
    crs: CRSInput | None = None,
    edges: EdgesInput | None = None,
) -> GeoArrowType: ...
def multipolygon(
    dimension: DimensionInput,
    coord_type: CoordTypeInput,
    *,
    crs: CRSInput | None = None,
    edges: EdgesInput | None = None,
) -> GeoArrowType: ...
def geometrycollection(
    dimension: DimensionInput,
    coord_type: CoordTypeInput,
    *,
    crs: CRSInput | None = None,
    edges: EdgesInput | None = None,
) -> GeoArrowType: ...
def geometry(
    coord_type: CoordType,
    *,
    crs: CRSInput | None = None,
    edges: EdgesInput | None = None,
) -> GeoArrowType: ...
def box(
    dimension: DimensionInput,
    *ICoordTypeInput,
    crs: CRSInput | None = None,
    edges: EdgesInput | None = None,
) -> GeoArrowType: ...
def wkb(
    *,
    crs: CRSInput | None = None,
    edges: EdgesInput | None = None,
) -> GeoArrowType: ...
def wkt(
    *,
    crs: CRSInput | None = None,
    edges: EdgesInput | None = None,
) -> GeoArrowType: ...
