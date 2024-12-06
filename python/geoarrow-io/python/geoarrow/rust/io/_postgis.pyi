from __future__ import annotations

from typing import Optional

from arro3.core import Table

def read_postgis(connection_url: str, sql: str) -> Optional[Table]:
    """
    Read a PostGIS query into an Arrow Table.

    Args:
        connection_url: _description_
        sql: _description_

    Returns:
        Table from query.
    """

async def read_postgis_async(connection_url: str, sql: str) -> Optional[Table]:
    """
    Read a PostGIS query into an Arrow Table.

    Args:
        connection_url: _description_
        sql: _description_

    Returns:
        Table from query.
    """
