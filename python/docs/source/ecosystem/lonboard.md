# Lonboard

[Lonboard](https://developmentseed.org/lonboard/latest/) is a Python library for fast, interactive geospatial vector data visualization in Jupyter. It's designed to be used with GeoArrow memory. Just pass the GeoTable to the `pyarrow.table` constructor and then pass that to the `table` argument of a layer.

```py
import pyarrow as pa
from geoarrow.rust.core import GeoTable, read_geojson
from lonboard import Map, PathLayer

path = "/path/to/file.geojson"
geo_table = read_geojson(path)
geo_table.geometry

layer = PathLayer(table=pa.table(geo_table))
m = Map(layers=[layer])
m
```

With the next release of lonboard, going through `pyarrow.table` will not be necessary.
