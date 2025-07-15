from __future__ import annotations

from typing import Tuple, overload

from arro3.core import Array, ChunkedArray, Table
from arro3.core.types import ArrowArrayExportable, ArrowStreamExportable
from geoarrow.rust.core import GeoArray, GeoChunkedArray

from .enums import AreaMethod, LengthMethod, RotateOrigin, SimplifyMethod
from .types import (
    AffineTransform,
    AreaMethodT,
    BroadcastGeometry,
    GeoInterfaceProtocol,
    LengthMethodT,
    NumpyArrayProtocolf64,
    RotateOriginT,
    SimplifyMethodT,
)

# Top-level array/chunked array functions

@overload
def affine_transform(
    input: ArrowArrayExportable, transform: AffineTransform
) -> GeoArray: ...
@overload
def affine_transform(
    input: ArrowStreamExportable, transform: AffineTransform
) -> GeoChunkedArray: ...
def affine_transform(
    input: ArrowArrayExportable | ArrowStreamExportable, transform: AffineTransform
) -> GeoArray | GeoChunkedArray:
    """
    Apply an affine transformation to geometries.

    This is intended to be equivalent to [`shapely.affinity.affine_transform`][] for 2D
    transforms.

    Args:
        input: input geometry array or chunked geometry array other: an affine
            transformation to apply to all geometries.

            This integrates with the [`affine`](https://github.com/rasterio/affine)
            Python library, and most users should use that integration, though it allows
            any input that is a tuple with 6 or 9 float values.

    Returns:
        New GeoArrow array or chunked array with the same type as input and with
        transformed coordinates.
    """

@overload
def area(
    input: ArrowArrayExportable,
    *,
    method: AreaMethod | AreaMethodT = AreaMethod.Euclidean,
) -> Array: ...
@overload
def area(
    input: ArrowStreamExportable,
    *,
    method: AreaMethod | AreaMethodT = AreaMethod.Euclidean,
) -> ChunkedArray: ...
def area(
    input: ArrowArrayExportable | ArrowStreamExportable,
    *,
    method: AreaMethod | AreaMethodT = AreaMethod.Euclidean,
) -> Array | ChunkedArray:
    """
    Determine the area of an array of geometries

    Args:
        input: input geometry array or chunked geometry array

    Other args:
        method: The method to use for area calculation. One of "Ellipsoidal", "Euclidean", or
            "Spherical". Refer to the documentation on
            [AreaMethod][geoarrow.rust.compute.enums.AreaMethod] for more information.

    Returns:
        Array or chunked array with area values.
    """

@overload
def center(input: ArrowArrayExportable) -> GeoArray: ...
@overload
def center(input: ArrowStreamExportable) -> GeoChunkedArray: ...
def center(
    input: ArrowArrayExportable | ArrowStreamExportable,
) -> GeoArray | GeoChunkedArray:
    """
    Compute the center of geometries

    This first computes the axis-aligned bounding rectangle, then takes the center of
    that box

    Args:
        input: input geometry array or chunked geometry array

    Returns:
        Array or chunked array with center values.
    """

@overload
def centroid(input: ArrowArrayExportable) -> GeoArray: ...
@overload
def centroid(input: ArrowStreamExportable) -> GeoChunkedArray: ...
def centroid(
    input: ArrowArrayExportable | ArrowStreamExportable,
) -> GeoArray | GeoChunkedArray:
    """
    Calculation of the centroid.

    The centroid is the arithmetic mean position of all points in the shape.
    Informally, it is the point at which a cutout of the shape could be perfectly
    balanced on the tip of a pin.

    The geometric centroid of a convex object always lies in the object.
    A non-convex object might have a centroid that _is outside the object itself_.

    Args:
        input: input geometry array or chunked geometry array

    Returns:
        Array or chunked array with centroid values.
    """

@overload
def chaikin_smoothing(input: ArrowArrayExportable, n_iterations: int) -> GeoArray: ...
@overload
def chaikin_smoothing(
    input: ArrowStreamExportable, n_iterations: int
) -> GeoChunkedArray: ...
def chaikin_smoothing(
    input: ArrowArrayExportable | ArrowStreamExportable,
    n_iterations: int,
) -> GeoArray | GeoChunkedArray:
    """
    Smoothen `LineString`, `Polygon`, `MultiLineString` and `MultiPolygon` using
    Chaikins algorithm.

    [Chaikins smoothing
    algorithm](http://www.idav.ucdavis.edu/education/CAGDNotes/Chaikins-Algorithm/Chaikins-Algorithm.html)

    Each iteration of the smoothing doubles the number of vertices of the geometry, so
    in some cases it may make sense to apply a simplification afterwards to remove
    insignificant coordinates.

    This implementation preserves the start and end vertices of an open linestring and
    smoothes the corner between start and end of a closed linestring.

    Args:
        input: input geometry array or chunked geometry array n_iterations: Number of
            iterations to use for smoothing.

    Returns:
        Smoothed geometry array or chunked geometry array.
    """

@overload
def convex_hull(input: ArrowArrayExportable) -> GeoArray: ...
@overload
def convex_hull(input: ArrowStreamExportable) -> GeoChunkedArray: ...
def convex_hull(
    input: ArrowArrayExportable | ArrowStreamExportable,
) -> GeoArray | GeoChunkedArray:
    """
    Returns the convex hull of a Polygon. The hull is always oriented
    counter-clockwise.

    This implementation uses the QuickHull algorithm, based on [Barber, C. Bradford;
    Dobkin, David P.; Huhdanpaa, Hannu (1 December
    1996)](https://dx.doi.org/10.1145%2F235815.235821) Original paper here:
    <http://www.cs.princeton.edu/~dpd/Papers/BarberDobkinHuhdanpaa.pdf>

    Args:
        input: input geometry array

    Returns:
        Array with convex hull polygons.
    """

@overload
def densify(input: ArrowArrayExportable, max_distance: float) -> GeoArray: ...
@overload
def densify(input: ArrowStreamExportable, max_distance: float) -> GeoChunkedArray: ...
def densify(
    input: ArrowArrayExportable, max_distance: float
) -> GeoArray | GeoChunkedArray:
    """
    Return a new linear geometry containing both existing and new interpolated
    coordinates with a maximum distance of `max_distance` between them.

    Note: `max_distance` must be greater than 0.

    Args:
        input: input geometry array
        max_distance: maximum distance between coordinates

    Returns:
        Densified geometry array
    """

@overload
def envelope(input: ArrowArrayExportable) -> GeoArray: ...
@overload
def envelope(input: ArrowStreamExportable) -> GeoChunkedArray: ...
def envelope(
    input: ArrowArrayExportable | ArrowStreamExportable,
) -> GeoArray | GeoChunkedArray:
    """
    Computes the minimum axis-aligned bounding box that encloses an input geometry

    Args:
        input: input geometry array

    Returns:
        Array with axis-aligned bounding boxes.
    """

@overload
def frechet_distance(
    input: ArrowArrayExportable,
    other: BroadcastGeometry,
) -> Array: ...
@overload
def frechet_distance(
    input: ArrowStreamExportable,
    other: BroadcastGeometry,
) -> ChunkedArray: ...
def frechet_distance(
    input: ArrowArrayExportable | ArrowStreamExportable,
    other: BroadcastGeometry,
) -> Array | ChunkedArray:
    """
    Determine the similarity between two arrays of `LineStrings` using the [Frechet
    distance].

    The Fréchet distance is a measure of similarity: it is the greatest distance between
    any point in A and the closest point in B. The discrete distance is an approximation
    of this metric: only vertices are considered. The parameter ‘densify’ makes this
    approximation less coarse by splitting the line segments between vertices before
    computing the distance.

    Fréchet distance sweep continuously along their respective curves and the direction
    of curves is significant. This makes it a better measure of similarity than
    Hausdorff distance for curve or surface matching.


    This implementation is based on [Computing Discrete Frechet Distance] by T. Eiter
    and H. Mannila.

    [Frechet distance]: https://en.wikipedia.org/wiki/Fr%C3%A9chet_distance [Computing
    Discrete Frechet Distance]:
    http://www.kr.tuwien.ac.at/staff/eiter/et-archive/cdtr9464.pdf

    Args:
        input: input geometry array or chunked geometry array other: the geometry or
            geometry array to compare against. This must contain geometries of
            `LineString` type. A variety of inputs are accepted:

            - Any Python class that implements the Geo Interface, such as a [`shapely`
              LineString][shapely.LineString]
            - Any GeoArrow array or chunked array of `LineString` type

    Returns:
        Array or chunked array with float distance values.
    """

@overload
def geodesic_perimeter(
    input: ArrowArrayExportable,
) -> Array: ...
@overload
def geodesic_perimeter(
    input: ArrowStreamExportable,
) -> ChunkedArray: ...
def geodesic_perimeter(
    input: ArrowArrayExportable | ArrowStreamExportable,
) -> Array | ChunkedArray:
    """
    Determine the perimeter of a geometry on an ellipsoidal model of the earth.

    This uses the geodesic measurement methods given by [Karney (2013)].

    For a polygon this returns the sum of the perimeter of the exterior ring and
    interior rings. To get the perimeter of just the exterior ring of a polygon, do
    `polygon.exterior().geodesic_length()`.

    ## Units

    - return value: meter

    [Karney (2013)]:  https://arxiv.org/pdf/1109.4448.pdf

    Returns:
        Array with output values.
    """

@overload
def is_empty(input: ArrowArrayExportable) -> Array: ...
@overload
def is_empty(input: ArrowStreamExportable) -> ChunkedArray: ...
def is_empty(
    input: ArrowArrayExportable | ArrowStreamExportable,
) -> Array | ChunkedArray:
    """
    Returns True if a geometry is an empty point, polygon, etc.

    Args:
        input: input geometry array

    Returns:
        Result array.
    """

@overload
def length(
    input: ArrowArrayExportable,
    *,
    method: LengthMethod | LengthMethodT = LengthMethod.Euclidean,
) -> Array: ...
@overload
def length(
    input: ArrowStreamExportable,
    *,
    method: LengthMethod | LengthMethodT = LengthMethod.Euclidean,
) -> ChunkedArray: ...
def length(
    input: ArrowArrayExportable | ArrowStreamExportable,
    *,
    method: LengthMethod | LengthMethodT = LengthMethod.Euclidean,
) -> Array | ChunkedArray:
    """
    Calculation of the length of a Line

    Args:
        input: input geometry array or chunked geometry array

    Other args:
        method: The method to use for length calculation. One of "Ellipsoidal",
            "Euclidean", "Haversine", or "Vincenty". Refer to the documentation on
            [LengthMethod][geoarrow.rust.compute.enums.LengthMethod] for more
            information. Defaults to LengthMethod.Euclidean.

    Returns:
        Array or chunked array with length values.
    """

@overload
def line_interpolate_point(
    input: ArrowArrayExportable,
    fraction: float | int | ArrowArrayExportable | NumpyArrayProtocolf64,
) -> GeoArray: ...
@overload
def line_interpolate_point(
    input: ArrowStreamExportable,
    fraction: float | int | ArrowStreamExportable,
) -> GeoChunkedArray: ...
def line_interpolate_point(
    input: ArrowArrayExportable | ArrowStreamExportable,
    fraction: float
    | int
    | ArrowArrayExportable
    | ArrowStreamExportable
    | NumpyArrayProtocolf64,
) -> GeoArray | GeoChunkedArray:
    """
    Returns a point interpolated at given distance on a line.

    This is intended to be equivalent to [`shapely.line_interpolate_point`][] when
    `normalized=True`.

    If the given fraction is

    * less than zero (including negative infinity): returns the starting point
    * greater than one (including infinity): returns the ending point
    * If either the fraction is NaN, or any coordinates of the line are not
    finite, returns `Point EMPTY`.

    Args:
        input: input geometry array or chunked geometry array
        fraction: the fractional distance along the line. A variety of inputs are accepted:

            - A Python `float` or `int`
            - A numpy `ndarray` with `float64` data type.
            - An Arrow array or chunked array with `float64` data type.

    Returns:
        PointArray or ChunkedPointArray with result values
    """

@overload
def line_locate_point(
    input: ArrowArrayExportable, point: GeoInterfaceProtocol | ArrowArrayExportable
) -> Array: ...
@overload
def line_locate_point(
    input: ArrowStreamExportable, point: GeoInterfaceProtocol | ArrowStreamExportable
) -> ChunkedArray: ...
def line_locate_point(
    input: ArrowArrayExportable | ArrowStreamExportable,
    point: GeoInterfaceProtocol | ArrowArrayExportable | ArrowStreamExportable,
) -> Array | ChunkedArray:
    """
    Returns a fraction of the line's total length
    representing the location of the closest point on the line to
    the given point.

    This is intended to be equivalent to [`shapely.line_locate_point`][] when
    `normalized=True`.

    If the line has zero length the fraction returned is zero.

    If either the point's coordinates or any coordinates of the line
    are not finite, returns `NaN`.

    Args:
        input: input geometry array or chunked geometry array
        point: the fractional distance along the line. A variety of inputs are accepted:

            - A scalar [`GeoScalar`][geoarrow.rust.core.GeoScalar]
            - A [`GeoArray`][geoarrow.rust.core.GeoArray]
            - A [`GeoChunkedArray`][geoarrow.rust.core.GeoChunkedArray]
            - Any Python class that implements the Geo Interface, such as a [`shapely` Point][shapely.Point]
            - Any GeoArrow array or chunked array of `Point` type

    Returns:
        Array or chunked array with float fraction values.
    """

@overload
def polylabel(
    input: ArrowArrayExportable,
    tolerance: float,
) -> GeoArray: ...
@overload
def polylabel(
    input: ArrowStreamExportable,
    tolerance: float,
) -> GeoChunkedArray: ...
def polylabel(
    input: ArrowArrayExportable | ArrowStreamExportable,
    tolerance: float,
) -> GeoArray | GeoChunkedArray:
    """
    Calculate a Polygon's ideal label position by calculating its _pole of inaccessibility_.

    The pole of inaccessibility is the most distant internal point from the polygon outline (not to
    be confused with centroid), and is useful for optimal placement of a text label on a polygon.

    The calculation uses an iterative grid-based algorithm, ported from the original [JavaScript
    implementation](https://github.com/mapbox/polylabel).

    Args:
        input: input geometry array or chunked geometry array
        tolerance: precision of algorithm. Refer to the [original JavaScript
            documentation](https://github.com/mapbox/polylabel/blob/07c112091b4c9ffeb412af33c575133168893b4a/README.md#how-the-algorithm-works)
            for more information

    Returns:
        PointArray or ChunkedPointArray with result values
    """

@overload
def signed_area(
    input: ArrowArrayExportable,
    *,
    method: AreaMethod | AreaMethodT = AreaMethod.Euclidean,
) -> Array: ...
@overload
def signed_area(
    input: ArrowStreamExportable,
    *,
    method: AreaMethod | AreaMethodT = AreaMethod.Euclidean,
) -> ChunkedArray: ...
def signed_area(
    input: ArrowArrayExportable | ArrowStreamExportable,
    *,
    method: AreaMethod | AreaMethodT = AreaMethod.Euclidean,
) -> Array | ChunkedArray:
    """
    Signed area of a geometry array

    Args:
        input: input geometry array or chunked geometry array

    Other args:
        method: The method to use for area calculation. One of "Ellipsoidal"
            "Euclidean", or "Spherical". Refer to the documentation on
            [AreaMethod][geoarrow.rust.compute.enums.AreaMethod] for more information.

    Returns:
        Array or chunked array with area values.
    """

@overload
def rotate(
    geom: ArrowArrayExportable,
    angle: float,
    *,
    origin: RotateOrigin | RotateOriginT | tuple[float, float],
) -> GeoArray: ...
@overload
def rotate(
    geom: ArrowStreamExportable,
    angle: float,
    *,
    origin: RotateOrigin | RotateOriginT | tuple[float, float],
) -> GeoChunkedArray: ...
def rotate(
    geom: ArrowArrayExportable | ArrowStreamExportable,
    angle: float,
    *,
    origin: RotateOrigin | RotateOriginT | tuple[float, float],
) -> GeoArray | GeoChunkedArray: ...
@overload
def scale(geom: ArrowArrayExportable, xfact: float, yfact: float) -> GeoArray: ...
@overload
def scale(
    geom: ArrowStreamExportable, xfact: float, yfact: float
) -> GeoChunkedArray: ...
def scale(
    geom: ArrowArrayExportable | ArrowStreamExportable, xfact: float, yfact: float
) -> GeoArray | GeoChunkedArray:
    """Returns a scaled geometry, scaled by factors along each dimension.

    Args:
        geom: _description_
        xfact: _description_
        yfact: _description_

    Returns:
        _description_
    """

@overload
def simplify(
    input: ArrowArrayExportable,
    epsilon: float,
    *,
    method: SimplifyMethod | SimplifyMethodT = SimplifyMethod.RDP,
) -> GeoArray: ...
@overload
def simplify(
    input: ArrowStreamExportable,
    epsilon: float,
    *,
    method: SimplifyMethod | SimplifyMethodT = SimplifyMethod.RDP,
) -> GeoChunkedArray: ...
def simplify(
    input: ArrowArrayExportable | ArrowStreamExportable,
    epsilon: float,
    *,
    method: SimplifyMethod | SimplifyMethodT = SimplifyMethod.RDP,
) -> GeoArray | GeoChunkedArray:
    """
    Simplifies a geometry.

    Args:
        input: input geometry array
        epsilon: tolerance for simplification. An epsilon less than or equal to zero will return an
            unaltered version of the geometry.

    Other args:
        method: The method to use for simplification calculation. One of `"rdp"`, `"vw"`, or
            `"vw_preserve"`. Refer to the documentation on
            [SimplifyMethod][geoarrow.rust.compute.enums.SimplifyMethod] for more
            information. Defaults to SimplifyMethod.RDP.

    Returns:
        Simplified geometry array.
    """

@overload
def skew(geom: ArrowArrayExportable, xs: float, ys: float) -> GeoArray: ...
@overload
def skew(geom: ArrowStreamExportable, xs: float, ys: float) -> GeoChunkedArray: ...
def skew(
    geom: ArrowArrayExportable | ArrowStreamExportable, xs: float, ys: float
) -> GeoArray | GeoChunkedArray:
    """
    Skew a geometry from it's bounding box center, using different values for `xs` and
    `ys` to distort the geometry's [aspect
    ratio](https://en.wikipedia.org/wiki/Aspect_ratio).

    Args:
        geom: _description_
        xs: _description_
        ys: _description_

    Returns:
        _description_
    """

@overload
def translate(geom: ArrowArrayExportable, xoff: float, yoff: float) -> GeoArray: ...
@overload
def translate(
    geom: ArrowStreamExportable, xoff: float, yoff: float
) -> GeoChunkedArray: ...
def translate(
    geom: ArrowArrayExportable | ArrowStreamExportable, xoff: float, yoff: float
) -> GeoArray | GeoChunkedArray:
    """Returns a scaled geometry, scaled by factors along each dimension.

    Args:
        geom: _description_
        xoff: _description_
        yoff: _description_

    Returns:
        _description_
    """

def total_bounds(
    input: ArrowArrayExportable | ArrowStreamExportable,
) -> Tuple[float, float, float, float]:
    """
    Computes the total bounds (extent) of the geometry.

    Args:
        input: input geometry array

    Returns:
        tuple of (xmin, ymin, xmax, ymax).
    """

# Top-level table functions

def explode(input: ArrowStreamExportable) -> Table:
    """
    Explode a table.

    This is intended to be equivalent to the [`explode`][geopandas.GeoDataFrame.explode] function
    in GeoPandas.

    Args:
        input: input table

    Returns:
        A new table with multi-part geometries exploded to separate rows.
    """
