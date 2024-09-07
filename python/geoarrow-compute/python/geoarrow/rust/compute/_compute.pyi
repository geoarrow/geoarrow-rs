from __future__ import annotations

from pathlib import Path
from typing import Any, Sequence, Tuple, overload

import geopandas as gpd
import numpy as np
from arro3.core import Array, ChunkedArray, RecordBatchReader, Table
from arro3.core.types import ArrowArrayExportable, ArrowStreamExportable
from geoarrow.rust.core import ChunkedGeometryArray, GeometryArray
from numpy.typing import NDArray

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
) -> GeometryArray: ...
@overload
def affine_transform(
    input: ArrowStreamExportable, transform: AffineTransform
) -> ChunkedGeometryArray: ...
def affine_transform(
    input: ArrowArrayExportable | ArrowStreamExportable, transform: AffineTransform
) -> GeometryArray | ChunkedGeometryArray:
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
            [AreaMethod][geoarrow.rust.core.enums.AreaMethod] for more information.

    Returns:
        Array or chunked array with area values.
    """

@overload
def center(input: ArrowArrayExportable) -> GeometryArray: ...
@overload
def center(input: ArrowStreamExportable) -> ChunkedGeometryArray: ...
def center(
    input: ArrowArrayExportable | ArrowStreamExportable,
) -> GeometryArray | ChunkedGeometryArray:
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
def centroid(input: ArrowArrayExportable) -> GeometryArray: ...
@overload
def centroid(input: ArrowStreamExportable) -> ChunkedGeometryArray: ...
def centroid(
    input: ArrowArrayExportable | ArrowStreamExportable,
) -> GeometryArray | ChunkedGeometryArray:
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
def chaikin_smoothing(
    input: ArrowArrayExportable, n_iterations: int
) -> GeometryArray: ...
@overload
def chaikin_smoothing(
    input: ArrowStreamExportable, n_iterations: int
) -> ChunkedGeometryArray: ...
def chaikin_smoothing(
    input: ArrowArrayExportable | ArrowStreamExportable,
    n_iterations: int,
) -> GeometryArray | ChunkedGeometryArray:
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
def convex_hull(input: ArrowArrayExportable) -> GeometryArray: ...
@overload
def convex_hull(input: ArrowStreamExportable) -> ChunkedGeometryArray: ...
def convex_hull(
    input: ArrowArrayExportable | ArrowStreamExportable,
) -> GeometryArray | ChunkedGeometryArray:
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
def densify(input: ArrowArrayExportable, max_distance: float) -> GeometryArray: ...
@overload
def densify(
    input: ArrowStreamExportable, max_distance: float
) -> ChunkedGeometryArray: ...
def densify(
    input: ArrowArrayExportable, max_distance: float
) -> GeometryArray | ChunkedGeometryArray:
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
def envelope(input: ArrowArrayExportable) -> GeometryArray: ...
@overload
def envelope(input: ArrowStreamExportable) -> ChunkedGeometryArray: ...
def envelope(
    input: ArrowArrayExportable | ArrowStreamExportable,
) -> GeometryArray | ChunkedGeometryArray:
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
            `LineString`` type. A variety of inputs are accepted:

            - A scalar [`LineString`][geoarrow.rust.core.LineString]
            - A [`LineStringArray`][geoarrow.rust.core.LineStringArray]
            - A [`ChunkedLineStringArray`][geoarrow.rust.core.ChunkedLineStringArray]
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
            [LengthMethod][geoarrow.rust.core.enums.LengthMethod] for more information.
            Defaults to LengthMethod.Euclidean.

    Returns:
        Array or chunked array with length values.
    """

@overload
def line_interpolate_point(
    input: ArrowArrayExportable,
    fraction: float | int | ArrowArrayExportable | NumpyArrayProtocolf64,
) -> GeometryArray: ...
@overload
def line_interpolate_point(
    input: ArrowStreamExportable,
    fraction: float | int | ArrowStreamExportable,
) -> ChunkedGeometryArray: ...
def line_interpolate_point(
    input: ArrowArrayExportable | ArrowStreamExportable,
    fraction: float
    | int
    | ArrowArrayExportable
    | ArrowStreamExportable
    | NumpyArrayProtocolf64,
) -> GeometryArray | ChunkedGeometryArray:
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

            - A scalar [`Point`][geoarrow.rust.core.Point]
            - A [`PointArray`][geoarrow.rust.core.PointArray]
            - A [`ChunkedPointArray`][geoarrow.rust.core.ChunkedPointArray]
            - Any Python class that implements the Geo Interface, such as a [`shapely` Point][shapely.Point]
            - Any GeoArrow array or chunked array of `Point` type

    Returns:
        Array or chunked array with float fraction values.
    """

@overload
def polylabel(
    input: ArrowArrayExportable,
    tolerance: float,
) -> GeometryArray: ...
@overload
def polylabel(
    input: ArrowStreamExportable,
    tolerance: float,
) -> ChunkedGeometryArray: ...
def polylabel(
    input: ArrowArrayExportable | ArrowStreamExportable,
    tolerance: float,
) -> GeometryArray | ChunkedGeometryArray:
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
         method: The method to use for area calculation. One of "Ellipsoidal", "Euclidean", or
            "Spherical". Refer to the documentation on
            [AreaMethod][geoarrow.rust.core.enums.AreaMethod] for more information.

    Returns:
        Array or chunked array with area values.
    """

@overload
def rotate(
    geom: ArrowArrayExportable,
    angle: float,
    *,
    origin: RotateOrigin | RotateOriginT | tuple[float, float],
) -> GeometryArray: ...
@overload
def rotate(
    geom: ArrowStreamExportable,
    angle: float,
    *,
    origin: RotateOrigin | RotateOriginT | tuple[float, float],
) -> ChunkedGeometryArray: ...
def rotate(
    geom: ArrowArrayExportable | ArrowStreamExportable,
    angle: float,
    *,
    origin: RotateOrigin | RotateOriginT | tuple[float, float],
) -> GeometryArray | ChunkedGeometryArray: ...
@overload
def scale(geom: ArrowArrayExportable, xfact: float, yfact: float) -> GeometryArray: ...
@overload
def scale(
    geom: ArrowStreamExportable, xfact: float, yfact: float
) -> ChunkedGeometryArray: ...
def scale(
    geom: ArrowArrayExportable | ArrowStreamExportable, xfact: float, yfact: float
) -> GeometryArray | ChunkedGeometryArray:
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
) -> GeometryArray: ...
@overload
def simplify(
    input: ArrowStreamExportable,
    epsilon: float,
    *,
    method: SimplifyMethod | SimplifyMethodT = SimplifyMethod.RDP,
) -> ChunkedGeometryArray: ...
def simplify(
    input: ArrowArrayExportable | ArrowStreamExportable,
    epsilon: float,
    *,
    method: SimplifyMethod | SimplifyMethodT = SimplifyMethod.RDP,
) -> GeometryArray | ChunkedGeometryArray:
    """
    Simplifies a geometry.

    Args:
        input: input geometry array
        epsilon: tolerance for simplification. An epsilon less than or equal to zero will return an
            unaltered version of the geometry.

    Other args:
        method: The method to use for simplification calculation. One of `"rdp"`, `"vw"`, or
            `"vw_preserve"`. Refer to the documentation on
            [SimplifyMethod][geoarrow.rust.core.enums.SimplifyMethod] for more information. Defaults to SimplifyMethod.RDP.

    Returns:
        Simplified geometry array.
    """

@overload
def skew(geom: ArrowArrayExportable, xs: float, ys: float) -> GeometryArray: ...
@overload
def skew(geom: ArrowStreamExportable, xs: float, ys: float) -> ChunkedGeometryArray: ...
def skew(
    geom: ArrowArrayExportable | ArrowStreamExportable, xs: float, ys: float
) -> GeometryArray | ChunkedGeometryArray:
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
def translate(
    geom: ArrowArrayExportable, xoff: float, yoff: float
) -> GeometryArray: ...
@overload
def translate(
    geom: ArrowStreamExportable, xoff: float, yoff: float
) -> ChunkedGeometryArray: ...
def translate(
    geom: ArrowArrayExportable | ArrowStreamExportable, xoff: float, yoff: float
) -> GeometryArray | ChunkedGeometryArray:
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

# Interop

def read_pyogrio(
    path_or_buffer: Path | str | bytes,
    /,
    layer: int | str | None = None,
    encoding: str | None = None,
    columns: Sequence[str] | None = None,
    read_geometry: bool = True,
    skip_features: int = 0,
    max_features: int | None = None,
    where: str | None = None,
    bbox: Tuple[float, float, float, float] | Sequence[float] | None = None,
    mask=None,
    fids=None,
    sql: str | None = None,
    sql_dialect: str | None = None,
    return_fids=False,
    batch_size=65536,
    **kwargs,
) -> RecordBatchReader:
    """
    Read from an OGR data source to an Arrow Table

    Args:
        path_or_buffer: A dataset path or URI, or raw buffer.
        layer: If an integer is provided, it corresponds to the index of the layer
            with the data source. If a string is provided, it must match the name
            of the layer in the data source. Defaults to first layer in data source.
        encoding: If present, will be used as the encoding for reading string values from
            the data source, unless encoding can be inferred directly from the data
            source.
        columns: List of column names to import from the data source. Column names must
            exactly match the names in the data source, and will be returned in
            the order they occur in the data source. To avoid reading any columns,
            pass an empty list-like.
        read_geometry: If True, will read geometry into a GeoSeries. If False, a Pandas DataFrame
            will be returned instead. Default: `True`.
        skip_features: Number of features to skip from the beginning of the file before
            returning features. If greater than available number of features, an
            empty DataFrame will be returned. Using this parameter may incur
            significant overhead if the driver does not support the capability to
            randomly seek to a specific feature, because it will need to iterate
            over all prior features.
        max_features: Number of features to read from the file. Default: `None`.
        where: Where clause to filter features in layer by attribute values. If the data source
            natively supports SQL, its specific SQL dialect should be used (eg. SQLite and
            GeoPackage: [`SQLITE`][SQLITE], PostgreSQL). If it doesn't, the [`OGRSQL
            WHERE`][OGRSQL_WHERE] syntax should be used. Note that it is not possible to overrule
            the SQL dialect, this is only possible when you use the `sql` parameter.

            Examples: `"ISO_A3 = 'CAN'"`, `"POP_EST > 10000000 AND POP_EST < 100000000"`

            [SQLITE]: https://gdal.org/user/sql_sqlite_dialect.html#sql-sqlite-dialect
            [OGRSQL_WHERE]: https://gdal.org/user/ogr_sql_dialect.html#where

        bbox: If present, will be used to filter records whose geometry intersects this
            box. This must be in the same CRS as the dataset. If GEOS is present
            and used by GDAL, only geometries that intersect this bbox will be
            returned; if GEOS is not available or not used by GDAL, all geometries
            with bounding boxes that intersect this bbox will be returned.
            Cannot be combined with `mask` keyword.
        mask: Shapely geometry, optional (default: `None`)
            If present, will be used to filter records whose geometry intersects
            this geometry. This must be in the same CRS as the dataset. If GEOS is
            present and used by GDAL, only geometries that intersect this geometry
            will be returned; if GEOS is not available or not used by GDAL, all
            geometries with bounding boxes that intersect the bounding box of this
            geometry will be returned. Requires Shapely >= 2.0.
            Cannot be combined with `bbox` keyword.
        fids : array-like, optional (default: `None`)
            Array of integer feature id (FID) values to select. Cannot be combined
            with other keywords to select a subset (`skip_features`,
            `max_features`, `where`, `bbox`, `mask`, or `sql`). Note that
            the starting index is driver and file specific (e.g. typically 0 for
            Shapefile and 1 for GeoPackage, but can still depend on the specific
            file). The performance of reading a large number of features usings FIDs
            is also driver specific.
        sql: The SQL statement to execute. Look at the sql_dialect parameter for more
            information on the syntax to use for the query. When combined with other
            keywords like `columns`, `skip_features`, `max_features`,
            `where`, `bbox`, or `mask`, those are applied after the SQL query.
            Be aware that this can have an impact on performance, (e.g. filtering
            with the `bbox` or `mask` keywords may not use spatial indexes).
            Cannot be combined with the `layer` or `fids` keywords.
        sql_dialect : str, optional (default: `None`)
            The SQL dialect the SQL statement is written in. Possible values:

            - **None**: if the data source natively supports SQL, its specific SQL dialect
                will be used by default (eg. SQLite and Geopackage: [`SQLITE`][SQLITE], PostgreSQL).
                If the data source doesn't natively support SQL, the [`OGRSQL`][OGRSQL] dialect is
                the default.
            - [`'OGRSQL'`][OGRSQL]: can be used on any data source. Performance can suffer
                when used on data sources with native support for SQL.
            - [`'SQLITE'`][SQLITE]: can be used on any data source. All [spatialite][spatialite]
                functions can be used. Performance can suffer on data sources with
                native support for SQL, except for Geopackage and SQLite as this is
                their native SQL dialect.

            [OGRSQL]: https://gdal.org/user/ogr_sql_dialect.html#ogr-sql-dialect
            [SQLITE]: https://gdal.org/user/sql_sqlite_dialect.html#sql-sqlite-dialect
            [spatialite]: https://www.gaia-gis.it/gaia-sins/spatialite-sql-latest.html

        **kwargs
            Additional driver-specific dataset open options passed to OGR. Invalid
            options will trigger a warning.

    Returns:
        Table
    """

def from_ewkb(input: ArrowArrayExportable) -> GeometryArray:
    """
    Parse an Arrow BinaryArray from EWKB to its GeoArrow-native counterpart.

    Args:
        input: An Arrow array of Binary type holding EWKB-formatted geometries.

    Returns:
        A GeoArrow-native geometry array
    """

def from_geopandas(input: gpd.GeoDataFrame) -> Table:
    """
    Create a GeoArrow Table from a [GeoPandas GeoDataFrame][geopandas.GeoDataFrame].

    ### Notes:

    - Currently this will always generate a non-chunked GeoArrow array. This is partly because
    [pyarrow.Table.from_pandas][pyarrow.Table.from_pandas] always creates a single batch.

    Args:
        input: A [GeoPandas GeoDataFrame][geopandas.GeoDataFrame].

    Returns:
        A GeoArrow Table
    """

def from_shapely(input, *, crs: Any | None = None) -> GeometryArray:
    """
    Create a GeoArrow array from an array of Shapely geometries.

    ### Notes:

    - Currently this will always generate a non-chunked GeoArrow array. Use the `from_shapely`
    method on a chunked GeoArrow array class to construct a chunked array.
    - This will first call [`to_ragged_array`][shapely.to_ragged_array], falling back to
    [`to_wkb`][shapely.to_wkb] if necessary. If you know you have mixed-type geometries in your
    column, use
    [`MixedGeometryArray.from_shapely`][geoarrow.rust.core.MixedGeometryArray.from_shapely].

    This is because `to_ragged_array` is the fastest approach but fails on mixed-type geometries.
    It supports combining Multi-* geometries with non-multi-geometries in the same array, so you
    can combine e.g. Point and MultiPoint geometries in the same array, but `to_ragged_array`
    doesn't work if you have Point and Polygon geometries in the same array.

    Args:

    input: Any array object accepted by Shapely, including numpy object arrays and
    [`geopandas.GeoSeries`][geopandas.GeoSeries].

    Returns:

        A GeoArrow array
    """

@overload
def from_wkb(
    input: ArrowArrayExportable,
    *,
    coord_type: CoordType | CoordTypeT = CoordType.Interleaved,
) -> GeometryArray: ...
@overload
def from_wkb(
    input: ArrowStreamExportable,
    *,
    coord_type: CoordType | CoordTypeT = CoordType.Interleaved,
) -> ChunkedGeometryArray: ...
def from_wkb(
    input: ArrowArrayExportable | ArrowStreamExportable,
    *,
    coord_type: CoordType | CoordTypeT = CoordType.Interleaved,
) -> GeometryArray | ChunkedGeometryArray:
    """
    Parse an Arrow BinaryArray from WKB to its GeoArrow-native counterpart.

    This expects ISO-formatted WKB geometries.

    Args:
        input: An Arrow array of Binary type holding WKB-formatted geometries.

    Other args:
        coord_type: Specify the coordinate type of the generated GeoArrow data.

    Returns:
        A GeoArrow-native geometry array
    """

@overload
def from_wkt(
    input: ArrowArrayExportable,
    *,
    coord_type: CoordType | CoordTypeT = CoordType.Interleaved,
) -> GeometryArray: ...
@overload
def from_wkt(
    input: ArrowStreamExportable,
    *,
    coord_type: CoordType | CoordTypeT = CoordType.Interleaved,
) -> ChunkedGeometryArray: ...
def from_wkt(
    input: ArrowArrayExportable | ArrowStreamExportable,
    *,
    coord_type: CoordType | CoordTypeT = CoordType.Interleaved,
) -> GeometryArray | ChunkedGeometryArray:
    """
    Parse an Arrow StringArray from WKT to its GeoArrow-native counterpart.

    Args:
        input: An Arrow array of string type holding WKT-formatted geometries.

    Other args:
        coord_type: Specify the coordinate type of the generated GeoArrow data.

    Returns:
        A GeoArrow-native geometry array
    """

def to_geopandas(input: ArrowStreamExportable) -> gpd.GeoDataFrame:
    """
    Convert a GeoArrow Table to a [GeoPandas GeoDataFrame][geopandas.GeoDataFrame].

    ### Notes:

    - This is an alias to [GeoDataFrame.from_arrow][geopandas.GeoDataFrame.from_arrow].

    Args:
    input: A GeoArrow Table.

    Returns:
        the converted GeoDataFrame
    """

def to_shapely(
    input: ArrowArrayExportable | ArrowStreamExportable,
) -> NDArray[np.object_]:
    """
    Convert a GeoArrow array to a numpy array of Shapely objects

    Args:
        input: input geometry array

    Returns:
        numpy array with Shapely objects
    """

@overload
def to_wkb(input: ArrowArrayExportable) -> GeometryArray: ...
@overload
def to_wkb(input: ArrowStreamExportable) -> ChunkedGeometryArray: ...
def to_wkb(input: ArrowArrayExportable) -> GeometryArray:
    """
    Encode a GeoArrow-native geometry array to a WKBArray, holding ISO-formatted WKB geometries.

    Args:
        input: A GeoArrow-native geometry array

    Returns:
        An array with WKB-formatted geometries
    """
