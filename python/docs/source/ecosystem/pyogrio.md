# Pyogrio/GDAL

Pyogrio has a [`read_arrow`](https://pyogrio.readthedocs.io/en/latest/api.html#pyogrio.raw.read_arrow) method that uses OGR to read in any supported vector file as an Arrow Table. We can pass that table to the `from_arrow` method on `GeoTable`.

```py
from geoarrow.rust.core import GeoTable
from pyogrio.raw import read_arrow

path = "path/to/file.shp"
meta, table = read_arrow(path)

table = GeoTable.from_arrow(table)
table.geometry
# <geoarrow.rust.core.rust.ChunkedMultiLineStringArray at 0x13fb61e70>
```
