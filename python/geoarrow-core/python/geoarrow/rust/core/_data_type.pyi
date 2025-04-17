from arro3.core.types import ArrowSchemaExportable
from geoarrow.rust.core.enums import CoordType, Dimension, Edges
from pyproj.crs.crs import CRS

class GeoArrowType:
    """
    GeoArrowType is a wrapper around the GeoArrowType C++ class.
    """

    def __init__(self, data: object) -> None:
        pass

    def __arrow_c_schema__(self) -> object:
        pass

    def __eq__(self, other: object) -> bool:
        pass

    def __repr__(self) -> str:
        pass

    @classmethod
    def from_arrow(cls, data: ArrowSchemaExportable) -> GeoArrowType:
        pass

    @classmethod
    def from_arrow_pycapsule(cls, capsule: object) -> GeoArrowType:
        pass

    @property
    def coord_type(self) -> CoordType: ...
    @property
    def dimension(self) -> Dimension: ...
    @property
    def crs(self) -> CRS | None: ...
    @property
    def edges(self) -> Edges | None: ...

def point(
    dimension: Dimension,
    coord_type: CoordType,
    *,
    crs: CRS | None = None,
    edges: Edges | None = None,
) -> GeoArrowType: ...
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
