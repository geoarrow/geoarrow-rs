from typing import List, Tuple

from arro3.core.types import ArrayInput
from geoarrow.rust.core.types import CRSInput

from ._array import GeoArray

CoordsInput = ArrayInput | Tuple[ArrayInput, ...] | List[ArrayInput]
"""Allowed coordinate input types.

Supported input:

- Tuple or list of arrays, with each element being a primitive array containing a single coordinate dimension: `(x, y)` or `(x, y, z)`. This will create a "separated" GeoArrow coordinate array. Each of these underlying arrays can be an Arrow or Numpy array.
- An Arrow fixed size list array with list size 2 or 3. This will create an "interleaved" GeoArrow coordinate array.
- A Numpy array of shape `(N, 2)` or `(N, 3)`. This will create an "interleaved" GeoArrow coordinate array. The array must be in C contiguous order.

Only float64 is supported as the numeric type of the coordinate arrays.
"""

def points(
    coords: CoordsInput,
    *,
    crs: CRSInput | None = None,
) -> GeoArray:
    """Create a GeoArrow point array from parts.

    This is similar in principle to [`shapely.points`][].

    Args:
        coords: Supported coordinate input, see [`CoordsInput`][geoarrow.rust.core.CoordsInput] for more information.

    Keyword Args:
        crs: The CRS to apply to the array. Defaults to None.

    Examples:
        ```py
        import numpy as np
        from geoarrow.rust.core import points

        # Creating a point array with interleaved coordinates
        coords = np.random.rand(10, 2)
        point_arr = points(coords)

        # Creating a point array with separated coordinates
        x_coords = np.random.rand(10)
        y_coords = np.random.rand(10)
        point_arr = points([x_coords, y_coords])

        # Creating a 3D point array with separated coordinates
        x_coords = np.random.rand(10)
        y_coords = np.random.rand(10)
        z_coords = np.random.rand(10)
        point_arr = points([x_coords, y_coords, z_coords])
        ```

    """

def linestrings(
    coords: CoordsInput,
    geom_offsets: ArrayInput,
    *,
    crs: CRSInput | None = None,
) -> GeoArray:
    """Create a GeoArrow linestring array from parts.

    This is similar in principle to [`shapely.linestrings`][].

    Args:
        coords: Supported coordinate input, see [`CoordsInput`][geoarrow.rust.core.CoordsInput] for more information.
        geom_offsets: The geometry offsets. Refer to the GeoArrow spec for more information.

    Keyword Args:
        crs: The CRS to apply to the array. Defaults to None.

    Examples:
        ```py
        import numpy as np
        from geoarrow.rust.core import linestrings

        coords = np.random.rand(10, 2)
        # The first LineString has 2 coordinates, the second and third LineString each
        # have 4 coordinates.
        geom_offsets = np.array([0, 2, 6, 10], dtype=np.int32)
        geom_arr = linestrings(coords, geom_offsets)
        ```

    """

def polygons(
    coords: CoordsInput,
    geom_offsets: ArrayInput,
    ring_offsets: ArrayInput,
    *,
    crs: CRSInput | None = None,
) -> GeoArray:
    """Create a GeoArrow polygon array from coordinates.

    This is similar in principle to [`shapely.polygons`][].

    Args:
        coords: Supported coordinate input, see [`CoordsInput`][geoarrow.rust.core.CoordsInput] for more information.
        geom_offsets: The geometry offsets. Refer to the GeoArrow spec for more information.
        ring_offsets: The ring offsets. Refer to the GeoArrow spec for more information.

    Keyword Args:
        crs: The CRS to apply to the array. Defaults to None.
    """

def multipoints(
    coords: CoordsInput,
    geom_offsets: ArrayInput,
    *,
    crs: CRSInput | None = None,
) -> GeoArray:
    """Create a GeoArrow multipoint array from parts.

    This is similar in principle to [`shapely.multipoints`][].

    Args:
        coords: Supported coordinate input, see [`CoordsInput`][geoarrow.rust.core.CoordsInput] for more information.
        geom_offsets: The geometry offsets. Refer to the GeoArrow spec for more information.

    Keyword Args:
        crs: The CRS to apply to the array. Defaults to None.
    """

def multilinestrings(
    coords: CoordsInput,
    geom_offsets: ArrayInput,
    ring_offsets: ArrayInput,
    *,
    crs: CRSInput | None = None,
) -> GeoArray:
    """Create a GeoArrow multilinestring array from parts.

    Args:
        coords: Supported coordinate input, see [`CoordsInput`][geoarrow.rust.core.CoordsInput] for more information.
        geom_offsets: The geometry offsets. Refer to the GeoArrow spec for more information.
        ring_offsets: The ring offsets. Refer to the GeoArrow spec for more information.

    Keyword Args:
        crs: The CRS to apply to the array. Defaults to None.
    """

def multipolygons(
    coords: CoordsInput,
    geom_offsets: ArrayInput,
    polygon_offsets: ArrayInput,
    ring_offsets: ArrayInput,
    *,
    crs: CRSInput | None = None,
) -> GeoArray:
    """Create a GeoArrow multipolygon array from parts.

    Args:
        coords: Supported coordinate input, see [`CoordsInput`][geoarrow.rust.core.CoordsInput] for more information.
        geom_offsets: The geometry offsets. Refer to the GeoArrow spec for more information.
        polygon_offsets: The polygon offsets. Refer to the GeoArrow spec for more information.
        ring_offsets: The ring offsets. Refer to the GeoArrow spec for more information.

    Keyword Args:
        crs: The CRS to apply to the array. Defaults to None.
    """
