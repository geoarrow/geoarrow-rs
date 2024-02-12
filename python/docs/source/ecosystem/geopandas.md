# GeoPandas

Use the [`from_geopandas`](../api/core/interop.md#geoarrow.rust.core.from_geopandas) and [`to_geopandas`](../api/core/interop.md#geoarrow.rust.core.to_geopandas) functions to convert a GeoTable from and to GeoPandas.

```py
import geopandas as gpd
from geoarrow.rust.core import from_geopandas, to_geopandas

gdf = gpd.GeoDataFrame(...)
table = from_geopandas(gdf)
back_to_geopandas_gdf = to_geopandas(table)
```
