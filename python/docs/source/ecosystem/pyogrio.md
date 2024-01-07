# Pyogrio/GDAL

Pyogrio has a [`read_arrow`](https://pyogrio.readthedocs.io/en/latest/api.html#pyogrio.raw.read_arrow) method that uses OGR to read in any supported vector file as an Arrow Table. We can pass that table to the [`from_arrow`](../api/core/table.md#geoarrow.rust.core.GeoTable.from_arrow) method on `GeoTable`.

```py
from geoarrow.rust.core import GeoTable
from pyogrio.raw import read_arrow

path = "path/to/file.shp"
meta, table = read_arrow(path)

table = GeoTable.from_arrow(table)
table.geometry
# <geoarrow.rust.core._rust.ChunkedMultiLineStringArray at 0x13fb61e70>
```

`read_arrow` returns a tuple of two elements: the first is a `dict` with metadata and the second is the actual table. For now, we ignore that metadata, but in the future, when CRS handling is added to this library, we'll be able to pass the metadata along.
