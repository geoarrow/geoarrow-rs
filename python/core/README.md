# geoarrow.rust.core

This library contains:

- standalone classes to represent GeoArrow arrays in rust: `PointArray`, `LineStringArray`, etc
- pure-rust algorithms (from [`georust/geo`](https://github.com/georust/geo)) that don't require a C extension module and can statically link on every platform.

In the future, this will also contain:

- Chunked classes: `ChunkedPointArray`, `ChunkedLineStringArray`, etc
- Table representations: `GeoTable`, where one of the columns is a geospatial type. This will support e.g. geospatial joins using Arrow memory.

Refer to the [GeoArrow Python module proposal](https://github.com/geoarrow/geoarrow-python/issues/38) for more information.
