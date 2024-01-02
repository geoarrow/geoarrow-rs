from __future__ import annotations

from typing import Protocol, Tuple


class ArrowArrayExportable(Protocol):
    """An Arrow or GeoArrow array from a local or remote (e.g. geoarrow.c) source."""

    def __arrow_c_array__(
        self, requested_schema: object | None = None
    ) -> Tuple[object, object]:
        ...
