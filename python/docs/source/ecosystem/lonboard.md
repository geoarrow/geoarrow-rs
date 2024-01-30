# Lonboard

[Lonboard](https://developmentseed.org/lonboard/latest/) is a Python library for fast, interactive geospatial vector data visualization in Jupyter.

[![](https://raw.githubusercontent.com/developmentseed/lonboard/main/assets/hero-image.jpg)](https://developmentseed.org/lonboard/latest/)

Lonboard was designed from the ground up to be used with GeoArrow and is the reason why Lonboard is fast.

Just pass a GeoTable as the `table` parameter of a layer.

## Examples

```py
import pyarrow as pa
from geoarrow.rust.core import GeoTable, read_geojson
from lonboard import Map, PathLayer

path = "/path/to/file.geojson"
geo_table = read_geojson(path)
geo_table.geometry

layer = PathLayer(table=geo_table)
m = Map(layer)
m
```

With the next release of lonboard, calling the `pyarrow.table` constructor will not be necessary.

Refer to [lonboard's documentation](https://developmentseed.org/lonboard/latest/) for more examples.
