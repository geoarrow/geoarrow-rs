# GeoPandas

Note that this loses the CRS information on the GeoDataFrame, but geoarrow-rust is not yet able to persist the CRS information.
```py
from geoarrow.rust.core import GeoTable
from pyarrow import Table

gdf = GeoDataFrame()
table = Table.from_pandas(gdf.to_wkb(flavor="iso"))
table = GeoTable.from_arrow(table) # (1)!
```

1. Hello world!

And back:

We need a to_wkb() on the table
```py
pyarrow_table = pa.table(geo_table)

```
