from __future__ import annotations

from pathlib import Path
from typing import Union

from arro3.core import Table
from geoarrow.rust.core.enums import CoordType
from geoarrow.rust.core.types import CoordTypeT

def read_shapefile(
    shp_path: Union[str, Path],
    *,
    batch_size: int = 65536,
    coord_type: CoordType | CoordTypeT | None = None,
) -> Table:
    """
    Read a Shapefile into an Arrow Table.

    The returned Arrow table will have geometry information in native GeoArrow encoding.

    Args:
        shp_path: the path to the `.shp` file

    Other args:
        batch_size: the number of rows to include in each internal batch of the table.
        coord_type: The coordinate type. Defaults to None.

    """
