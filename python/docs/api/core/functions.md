# Functions

Interoperability with other Python geospatial libraries (Shapely, GeoPandas) and in-memory geospatial formats (WKB, WKT).

::: geoarrow.rust.core
    options:
      filters:
        - "!^_"
      members:
        - read_pyogrio
        - from_geopandas
        - from_shapely
        - from_wkb
        - from_wkt
        - to_geopandas
        - to_shapely
        - to_wkb
        - to_wkt

## Table functions

::: geoarrow.rust.core
    options:
      filters:
        - "!^_"
      members:
        - geometry_col
