from __future__ import annotations

from typing import Literal, Protocol, Tuple, TypeVar, Union

from arro3.core.types import (
    ArrowArrayExportable,
    ArrowStreamExportable,
)
from geoarrow.rust.core import Geometry

try:
    import numpy as np
    from numpy.typing import NDArray

    ScalarType_co = TypeVar("ScalarType_co", bound=np.generic, covariant=True)

except ImportError:
    ScalarType_co = TypeVar("ScalarType_co", covariant=True)


IntFloat = Union[int, float]


AffineTransform = Union[
    Tuple[IntFloat, IntFloat, IntFloat, IntFloat, IntFloat, IntFloat],
    Tuple[
        IntFloat,
        IntFloat,
        IntFloat,
        IntFloat,
        IntFloat,
        IntFloat,
        IntFloat,
        IntFloat,
        IntFloat,
    ],
    Tuple[IntFloat, ...],
]

AreaMethodT = Literal["ellipsoidal", "euclidean", "spherical"]
"""Acceptable strings to be passed into the `method` parameter for
[`area`][geoarrow.rust.compute.area] and
[`signed_area`][geoarrow.rust.compute.signed_area].
"""

LengthMethodT = Literal["ellipsoidal", "euclidean", "haversine", "vincenty"]
"""Acceptable strings to be passed into the `method` parameter for
[`length`][geoarrow.rust.compute.length].
"""

RotateOriginT = Literal["center", "centroid"]
"""Acceptable strings to be passed into the `origin` parameter for
[`rotate`][geoarrow.rust.compute.rotate].
"""

SimplifyMethodT = Literal["rdp", "vw", "vw_preserve"]
"""Acceptable strings to be passed into the `method` parameter for
[`simplify`][geoarrow.rust.compute.simplify].
"""


class GeoInterfaceProtocol(Protocol):
    """A scalar geometry that implements the Geo Interface protocol."""

    @property
    def __geo_interface__(self) -> dict: ...


class NumpyArrayProtocolf64(Protocol):
    """An object that implements the numpy __array__ method."""

    def __array__(self) -> NDArray[np.float64]: ...


ScalarGeometry = Union[
    GeoInterfaceProtocol,
    Geometry,
]

BroadcastGeometry = Union[
    ScalarGeometry,
    ArrowArrayExportable,
    ArrowStreamExportable,
]
