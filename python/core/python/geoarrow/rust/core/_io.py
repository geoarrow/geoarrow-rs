import asyncio
from typing import Optional

from ._rust import GeoTable, read_postgis_async


def read_postgis(*args, **kwargs) -> Optional[GeoTable]:
    """Read a PostGIS query into a GeoTable.

    Returns:
        Table from query.
    """

    async def wrapper():
        return await read_postgis_async(*args, **kwargs)

    return asyncio.run(wrapper())
