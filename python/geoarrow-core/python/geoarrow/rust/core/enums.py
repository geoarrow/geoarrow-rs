from enum import Enum, IntEnum


class StrEnum(str, Enum):
    def __str__(self):
        return str(self.value)


class CoordType(StrEnum):
    """Available GeoArrow coordinate types."""

    INTERLEAVED = "interleaved"
    """Interleaved coordinate layout.

    All coordinates are stored in a single buffer, as `XYXYXY`.
    """

    SEPARATED = "separated"
    """Separated coordinate layout.

    Coordinates are stored in a separate buffer per dimension, e.g. `XXXX` and `YYYY`.
    """


class Dimension(StrEnum):
    """Geometry dimensions."""

    XY = "xy"
    """Two dimensions, X and Y
    """

    XYZ = "xyz"
    """Three dimensions, X, Y, and Z
    """

    XYM = "xym"
    """Three dimensions, X, Y, and M
    """

    XYZM = "xyzm"
    """Four dimensions, X, Y, Z, and M
    """


class Edges(StrEnum):
    """Edges."""

    ANDOYER = "andoyer"

    KARNEY = "karney"

    SPHERICAL = "spherical"

    THOMAS = "thomas"

    VINCENTY = "vincenty"


class GeometryType(IntEnum):
    GEOMETRY = 0
    """Unknown geometry type."""

    POINT = 1
    """Point geometry type."""

    LINESTRING = 2
    """Linestring geometry type."""

    POLYGON = 3
    """Polygon geometry type."""

    MULTIPOINT = 4
    """Multipoint geometry type."""

    MULTILINESTRING = 5
    """Multilinestring geometry type."""

    MULTIPOLYGON = 6
    """Multipolygon geometry type."""

    GEOMETRYCOLLECTION = 7
    """Geometrycollection geometry type."""

    BOX = 990
    """Box geometry type."""
