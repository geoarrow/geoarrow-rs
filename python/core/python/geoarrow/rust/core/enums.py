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


class AreaMethod(StrEnum):
    Ellipsoidal = auto()
    """Use an ellipsoidal model of the Earth for area calculations.

    This uses the geodesic measurement methods given by [Karney (2013)].

    ## Assumptions

    - Polygons are assumed to be wound in a counter-clockwise direction for the exterior
      ring and a clockwise direction for interior rings. This is the standard winding
      for geometries that follow the Simple Feature standard. Alternative windings may
      result in a negative area. See "Interpreting negative area values" below.
    - Polygons are assumed to be smaller than half the size of the earth. If you expect
      to be dealing with polygons larger than this, please use the `unsigned` methods.

    ## Units

    - return value: meter²

    ## Interpreting negative area values

    A negative value can mean one of two things:

    1. The winding of the polygon is in the clockwise direction (reverse winding). If
       this is the case, and you know the polygon is smaller than half the area of
       earth, you can take the absolute value of the reported area to get the correct
       area.
    2. The polygon is larger than half the planet. In this case, the returned area of
       the polygon is not correct. If you expect to be dealing with very large polygons,
       please use the `unsigned` methods.

    [Karney (2013)]:  https://arxiv.org/pdf/1109.4448.pdf
    """

    Euclidean = auto()
    """Calculate planar area."""

    Spherical = auto()
    """Use a spherical model of the Earth for area calculations.

    Calculate the geodesic area of a geometry on a sphere using the algorithm presented
    in _Some Algorithms for Polygons on a Sphere_ by [Chamberlain and Duquette (2007)].

    [Chamberlain and Duquette (2007)]: https://trs.jpl.nasa.gov/handle/2014/41271

    ## Units

    - return value: meter²
    """
