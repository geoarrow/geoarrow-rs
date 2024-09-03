# Shapely

For interoperability with [Shapely](https://shapely.readthedocs.io/en/stable/index.html), you have three options:

- Top-level [`to_shapely`](../api/core/interop.md#geoarrow.rust.core.to_shapely) and [`from_shapely`](../api/core/interop.md#geoarrow.rust.core.from_shapely) functions which aim to accept all array input.
- `to_shapely` and `from_shapely` methods available on geometry arrays
- `to_shapely` and `from_shapely` methods available on **chunked** geometry arrays

How to choose?

- If you know your data's geometry type, prefer the `from_shapely` method on a chunked array class. E.g. `ChunkedLineStringArray.from_shapely()`.

    This has type hinting and auto-completion benefits because your code editor knows what output array you'll receive. It also validates that your data match the geometry type you expect.

    It can also lead to better performance because many operations over chunked arrays are automatically multi-threading.
- Top-level functions should work with all GeoArrow data structures (except tables as Shapely is geometry-only). The downside is that

Shapely interoperability requires `shapely` to be installed, and requires Shapely version 2.0 or higher.

## Examples

```py
from geoarrow.rust.core import from_shapely, to_shapely
import shapely

shapely_geoms = to_shapely(geoarrow_array)
geoarrow_array = from_shapely(shapely_geoms)
```

Or, if you know the geometry type:

```py
from geoarrow.rust.core import ChunkedLineStringArray
import shapely

geoarrow_array = ChunkedLineStringArray.from_shapely(shapely_geoms)
shapely_geoms = geoarrow_array.to_shapely()
```
