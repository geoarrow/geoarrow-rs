# Lonboard

[Lonboard][lonboard_docs] is a Python library for fast, interactive geospatial vector data visualization in Jupyter.

[![](https://raw.githubusercontent.com/developmentseed/lonboard/main/assets/hero-image.jpg)][lonboard_docs]

Lonboard was designed from the ground up to be used with GeoArrow and is the reason why Lonboard is fast.

As of Lonboard version 0.6 or later, just pass a GeoTable as the `table` parameter of a layer.

## Examples

```py
from geoarrow.rust.core import read_geojson
from lonboard import Map, PathLayer

path = "/path/to/file.geojson"
geo_table = read_geojson(path)
geo_table.geometry

layer = PathLayer(table=geo_table)
m = Map(layer)
m
```

Refer to [lonboard's documentation][lonboard_docs] for more examples.

[lonboard_docs]: https://developmentseed.org/lonboard/latest/
