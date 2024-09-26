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

    def _generate_next_value_(name, *_):
        return name.lower()


class CoordType(StrEnum):
    """Available GeoArrow coordinate types."""

    Interleaved = auto()
    """Interleaved coordinate layout.

    All coordinates are stored in a single buffer, as `XYXYXY`.
    """

    Separated = auto()
    """Separated coordinate layout.

    Coordinates are stored in a separate buffer per dimension, e.g. `XXXX` and `YYYY`.
    """


class Dimension(StrEnum):
    """Geometry dimensions."""

    XY = auto()
    """Two dimensions, X and Y
    """

    XYZ = auto()
    """Three dimensions, X, Y, and Z
    """
