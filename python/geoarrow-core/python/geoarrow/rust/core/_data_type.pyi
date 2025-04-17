from arro3.core.types import ArrowSchemaExportable
from geoarrow.rust.core.enums import CoordType, Dimension, Edges
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
    dimension: Dimension,
    coord_type: CoordType,
    *,
    crs: CRS | None = None,
    edges: Edges | None = None,
) -> GeoArrowType:
    """Create a new Arrow type for a GeoArrow Point array."""

def linestring(
    dimension: Dimension,
    coord_type: CoordType,
    *,
    crs: CRS | None = None,
    edges: Edges | None = None,
) -> GeoArrowType: ...
def polygon(
    dimension: Dimension,
    coord_type: CoordType,
    *,
    crs: CRS | None = None,
    edges: Edges | None = None,
) -> GeoArrowType: ...
def multipoint(
    dimension: Dimension,
    coord_type: CoordType,
    *,
    crs: CRS | None = None,
    edges: Edges | None = None,
) -> GeoArrowType: ...
def multilinestring(
    dimension: Dimension,
    coord_type: CoordType,
    *,
    crs: CRS | None = None,
    edges: Edges | None = None,
) -> GeoArrowType: ...
def multipolygon(
    dimension: Dimension,
    coord_type: CoordType,
    *,
    crs: CRS | None = None,
    edges: Edges | None = None,
) -> GeoArrowType: ...
def geometrycollection(
    dimension: Dimension,
    coord_type: CoordType,
    *,
    crs: CRS | None = None,
    edges: Edges | None = None,
) -> GeoArrowType: ...
def geometry(
    coord_type: CoordType,
    *,
    crs: CRS | None = None,
    edges: Edges | None = None,
) -> GeoArrowType: ...
def box(
    dimension: Dimension,
    *,
    crs: CRS | None = None,
    edges: Edges | None = None,
) -> GeoArrowType: ...
def wkb(
    *,
    crs: CRS | None = None,
    edges: Edges | None = None,
) -> GeoArrowType: ...
def wkt(
    *,
    crs: CRS | None = None,
    edges: Edges | None = None,
) -> GeoArrowType: ...
