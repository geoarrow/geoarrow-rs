
TODO: seems a bit slow to create the PathLayer from arrow?

```py
from lonboard import PathLayer
import pyarrow as pa
from geoarrow.rust.core import GeoTable
from pyogrio.raw import read_arrow

path = "/Users/kyle/github/geoarrow/geoarrow-rs/python/core/ne_10m_roads_north_america/ne_10m_roads_north_america.shp"
meta, table = read_arrow(path)

geo_table = GeoTable.from_arrow(table)
geo_table.geometry

%time layer = PathLayer(table=pa.table(geo_table))

```
