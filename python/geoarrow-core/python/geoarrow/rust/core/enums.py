from enum import Enum, auto


class StrEnum(str, Enum):
    def __new__(cls, value, *args, **kwargs):
        if not isinstance(value, (str, auto)):
            raise TypeError(
                f"Values of StrEnums must be strings: {value!r} is a {type(value)}"
            )
        return super().__new__(cls, value, *args, **kwargs)

    def __str__(self):
        return str(self.value)


class CoordType(StrEnum):
    """Available GeoArrow coordinate types."""

    Interleaved = "interleaved"
    """Interleaved coordinate layout.

    All coordinates are stored in a single buffer, as `XYXYXY`.
    """

    Separated = "separated"
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
