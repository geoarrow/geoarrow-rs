# GeoPandas

For the time being, to move data from GeoPandas to geoarrow-rust, write it to GeoParquet and read it back. This interoperability will be improved in the next release.

```py
import geopandas as gpd
from geoarrow.rust.core import read_parquet

gdf = gpd.GeoDataFrame()
path = "temporary_file.parquet"
gdf.to_parquet(path)
geo_table = read_parquet(path)
```
