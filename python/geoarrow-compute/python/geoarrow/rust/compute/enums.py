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


class LengthMethod(StrEnum):
    Ellipsoidal = auto()
    """Determine the length of a geometry on an ellipsoidal model of the earth.

    This uses the geodesic measurement methods given by [Karney (2013)]. As opposed to
    older methods like Vincenty, this method is accurate to a few nanometers and always
    converges.

    [Karney (2013)]:  https://arxiv.org/pdf/1109.4448.pdf
    """

    Euclidean = auto()
    """Determine the length of a geometry using planar calculations."""

    Haversine = auto()
    """Determine the length of a geometry using the [haversine formula].

    [haversine formula]: https://en.wikipedia.org/wiki/Haversine_formula

    *Note*: this implementation uses a mean earth radius of 6371.088 km, based on the
    [recommendation of the IUGG](ftp://athena.fsv.cvut.cz/ZFG/grs80-Moritz.pdf)
    """

    Vincenty = auto()
    """Determine the length of a geometry using [Vincenty's formulae].

    [Vincenty's formulae]: https://en.wikipedia.org/wiki/Vincenty%27s_formulae
    """


class RotateOrigin(StrEnum):
    Center = auto()
    """Use the center of a geometry for rotation"""

    Centroid = auto()
    """Use the centroid of a geometry for rotation"""


class SimplifyMethod(StrEnum):
    RDP = auto()
    """Use the [Ramer-Douglas-Peucker
    algorithm](https://en.wikipedia.org/wiki/Ramer-Douglas-Peucker_algorithm) for
    LineString simplificatino.

    Polygons are simplified by running the RDP algorithm on
    all their constituent rings. This may result in invalid Polygons, and has no
    guarantee of preserving topology.

    Multi* objects are simplified by simplifying all their constituent geometries
    individually.
    """

    VW = auto()
    """Use the
    [Visvalingam-Whyatt](http://www.tandfonline.com/doi/abs/10.1179/000870493786962263)
    algorithm for LineString simplification.

    See [here](https://bost.ocks.org/mike/simplify/) for a graphical explanation
    Polygons are simplified by running the algorithm on all their constituent rings.

    This may result in invalid Polygons, and has no guarantee of preserving topology.
    Multi* objects are simplified by simplifying all their constituent geometries
    individually.
    """

    VW_Preserve = auto()
    """Use a topology-preserving variant of
    the
    [Visvalingam-Whyatt](http://www.tandfonline.com/doi/abs/10.1179/000870493786962263)
    algorithm for LineString simplification.

    See [here](https://www.jasondavies.com/simplify/) for a graphical explanation.

    The topology-preserving algorithm uses an R* tree to efficiently find candidate line
    segments which are tested for intersection with a given triangle. If intersections
    are found, the previous point (i.e. the left component of the current triangle) is
    also removed, altering the geometry and removing the intersection.

    # Notes

    - It is possible for the simplification algorithm to displace a Polygon's interior
      ring outside its shell.
    - The algorithm does **not** guarantee a valid output geometry, especially on
      smaller geometries.
    - If removal of a point causes a self-intersection, but the geometry only has `n +
      1` points remaining (3 for a `LineString`, 5 for a `Polygon`), the point is
      retained and the simplification process ends. This is because there is no
      guarantee that removal of two points will remove the intersection, but removal of
      further points would leave too few points to form a valid geometry.
    - The tolerance used to remove a point is `epsilon`, in keeping with GEOS. JTS uses
      `epsilon ^ 2`
    """
