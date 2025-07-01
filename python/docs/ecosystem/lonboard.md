# Lonboard

[Lonboard][lonboard_docs] is a Python library for fast, interactive geospatial vector data visualization in Jupyter.

[![](https://raw.githubusercontent.com/developmentseed/lonboard/main/assets/hero-image.jpg)][lonboard_docs]

Lonboard was designed from the ground up to be used with GeoArrow, and GeoArrow is the reason why Lonboard is fast.

You can pass a GeoArrow array, chunked array, or table object to Lonboard's [`viz`][lonboard.viz] and it should just work. Or, alternatively, pass a GeoArrow table as the `table` parameter of a layer's constructor, like in [`ScatterplotLayer.__init__`][lonboard.ScatterplotLayer.__init__].

!!! note
    Lonboard does not yet support the new Geometry and GeometryCollection array types introduced in GeoArrow specification version 0.2. It likely will soon. For now, use [`downcast`][geoarrow.rust.core.GeoArray.downcast] to simplify geometry types.

    Lonboard does also support the WKB array type, so you can use [`to_wkb`][geoarrow.rust.core.to_wkb] on a Geometry or GeometryCollection array.

## Examples

Passing a GeoArrow table to [`viz`][lonboard.viz]:

```py
from geoarrow.rust.io import GeoParquetFile
from obstore.store import HTTPStore
from lonboard import viz

store = HTTPStore.from_url(
    "https://raw.githubusercontent.com/opengeospatial/geoparquet/v1.0.0/examples"
)
file = GeoParquetFile.open("example.parquet", store=store)
table = file.read()

m = viz(table)
m
```

Refer to [lonboard's documentation][lonboard_docs] for more examples.

[lonboard_docs]: https://developmentseed.org/lonboard/latest/
