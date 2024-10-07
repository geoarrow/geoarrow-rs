from typing import List, Tuple

from arro3.core.types import ArrayInput

from ._rust import NativeArray

def points(
    coords: ArrayInput | Tuple[ArrayInput, ...] | List[ArrayInput],
) -> NativeArray: ...
def linestrings(
    coords: ArrayInput | Tuple[ArrayInput, ...] | List[ArrayInput],
    geom_offsets: ArrayInput,
) -> NativeArray: ...
def polygons(
    coords: ArrayInput | Tuple[ArrayInput, ...] | List[ArrayInput],
    geom_offsets: ArrayInput,
    ring_offsets: ArrayInput,
) -> NativeArray: ...
def multipoints(
    coords: ArrayInput | Tuple[ArrayInput, ...] | List[ArrayInput],
) -> NativeArray: ...
def multilinestrings(
    coords: ArrayInput | Tuple[ArrayInput, ...] | List[ArrayInput],
    geom_offsets: ArrayInput,
    ring_offsets: ArrayInput,
) -> NativeArray: ...
def multipolygons(
    coords: ArrayInput | Tuple[ArrayInput, ...] | List[ArrayInput],
    geom_offsets: ArrayInput,
    polygon_offsets: ArrayInput,
    ring_offsets: ArrayInput,
) -> NativeArray: ...
