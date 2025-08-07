from typing import overload
from arro3.core.types import ArrowArrayExportable, ArrowStreamExportable
from arro3.core import Array, ArrayReader

@overload
def get_type_id(input: ArrowArrayExportable) -> Array: ...
@overload
def get_type_id(input: ArrowStreamExportable) -> ArrayReader: ...
def get_type_id(
    input: ArrowArrayExportable | ArrowStreamExportable,
) -> Array | ArrayReader:
    """Returns integer type ids for each geometry in the input.

    The returned integers match the internal ids of the GeoArrow [Geometry
    type](https://geoarrow.org/format.html#geometry):

    | Type ID | Geometry type         |
    | ------- | --------------------- |
    | 1       | Point                 |
    | 2       | LineString            |
    | 3       | Polygon               |
    | 4       | MultiPoint            |
    | 5       | MultiLineString       |
    | 6       | MultiPolygon          |
    | 7       | GeometryCollection    |
    | 11      | Point Z               |
    | 12      | LineString Z          |
    | 13      | Polygon Z             |
    | 14      | MultiPoint Z          |
    | 15      | MultiLineString Z     |
    | 16      | MultiPolygon Z        |
    | 17      | GeometryCollection Z  |
    | 21      | Point M               |
    | 22      | LineString M          |
    | 23      | Polygon M             |
    | 24      | MultiPoint M          |
    | 25      | MultiLineString M     |
    | 26      | MultiPolygon M        |
    | 27      | GeometryCollection M  |
    | 31      | Point ZM              |
    | 32      | LineString ZM         |
    | 33      | Polygon ZM            |
    | 34      | MultiPoint ZM         |
    | 35      | MultiLineString ZM    |
    | 36      | MultiPolygon ZM       |
    | 37      | GeometryCollection ZM |

    !!! warning
        These ids do not exactly match the result of `shapely.get_type_id`. Shapely does
        not distinguish between dimensions. Also the ids differ for `Point` and
        `LineString` compared to here.

    Args:
        input: Input geometry array, chunked array, or stream.

    Returns:
        An int8 Array of type ids.
    """
