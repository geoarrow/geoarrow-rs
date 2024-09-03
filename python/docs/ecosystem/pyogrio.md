# Pyogrio/GDAL

Use the [`read_pyogrio`](../api/core/io.md#geoarrow.rust.core.read_pyogrio) function to read an OGR-supported data source through [pyogrio](https://pyogrio.readthedocs.io/en/latest/).

This requires the optional `pyogrio` and `pyarrow` dependencies.

```py
from geoarrow.rust.core import read_pyogrio, geometry_col

path = "path/to/file.shp"
table = read_pyogrio(path)
geometry = geometry_col(table)
# <geoarrow.rust.core._rust.ChunkedMultiLineStringArray at 0x13fb61e70>
```
