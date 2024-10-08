from typing import List, Tuple

from arro3.core.types import ArrayInput
from geoarrow.rust.core.types import CRSInput

from ._rust import NativeArray

def points(
    coords: ArrayInput | Tuple[ArrayInput, ...] | List[ArrayInput],
    *,
    crs: CRSInput | None = None,
) -> NativeArray: ...
def linestrings(
    coords: ArrayInput | Tuple[ArrayInput, ...] | List[ArrayInput],
    geom_offsets: ArrayInput,
    *,
    crs: CRSInput | None = None,
) -> NativeArray: ...
def polygons(
    coords: ArrayInput | Tuple[ArrayInput, ...] | List[ArrayInput],
    geom_offsets: ArrayInput,
    ring_offsets: ArrayInput,
    *,
    crs: CRSInput | None = None,
) -> NativeArray: ...
def multipoints(
    coords: ArrayInput | Tuple[ArrayInput, ...] | List[ArrayInput],
    *,
    crs: CRSInput | None = None,
) -> NativeArray: ...
def multilinestrings(
    coords: ArrayInput | Tuple[ArrayInput, ...] | List[ArrayInput],
    geom_offsets: ArrayInput,
    ring_offsets: ArrayInput,
    *,
    crs: CRSInput | None = None,
) -> NativeArray: ...
def multipolygons(
    coords: ArrayInput | Tuple[ArrayInput, ...] | List[ArrayInput],
    geom_offsets: ArrayInput,
    polygon_offsets: ArrayInput,
    ring_offsets: ArrayInput,
    *,
    crs: CRSInput | None = None,
) -> NativeArray: ...
