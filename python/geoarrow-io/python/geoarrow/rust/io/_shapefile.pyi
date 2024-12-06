from __future__ import annotations

from pathlib import Path
from typing import BinaryIO, Union

from arro3.core import Table

def read_shapefile(
    shp_file: Union[str, Path, BinaryIO], dbf_file: Union[str, Path, BinaryIO]
) -> Table:
    """
    Read a Shapefile into an Arrow Table.

    The returned Arrow table will have geometry information in native GeoArrow encoding.

    !!! note
        Coordinate Reference System information is not currently read from the Shapefile.

    Args:
        shp_file: the path to the `.shp` file or the `.shp` file as a Python file object in binary read mode.
        dbf_file: the path to the `.dbf` file or the `.dbf` file as a Python file object in binary read mode.
    """
