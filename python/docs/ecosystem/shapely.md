# Shapely

For interoperability with [Shapely](https://shapely.readthedocs.io/en/stable/index.html), use the top-level [`to_shapely`](../api/core/interop.md#geoarrow.rust.core.to_shapely) and [`from_shapely`](../api/core/interop.md#geoarrow.rust.core.from_shapely) functions.

Shapely interoperability requires `shapely` to be installed, and requires Shapely version 2.0 or higher.

## Examples

```py
from geoarrow.rust.core import from_shapely, to_shapely
import shapely

shapely_geoms = to_shapely(geoarrow_array)
geoarrow_array = from_shapely(shapely_geoms)
```
