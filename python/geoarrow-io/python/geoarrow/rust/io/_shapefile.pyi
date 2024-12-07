from __future__ import annotations

from pathlib import Path
from typing import Union

from arro3.core import Table

def read_shapefile(shp_path: Union[str, Path]) -> Table:
    """
    Read a Shapefile into an Arrow Table.

    The returned Arrow table will have geometry information in native GeoArrow encoding.

    Args:
        shp_file: the path to the `.shp` file or the `.shp` file as a Python file object in binary read mode.
        dbf_file: the path to the `.dbf` file or the `.dbf` file as a Python file object in binary read mode.
    """
