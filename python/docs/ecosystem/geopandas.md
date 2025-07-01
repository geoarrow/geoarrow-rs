# GeoPandas

As of GeoPandas v1.0, GeoPandas natively supports GeoArrow.

Use [`GeoDataFrame.to_arrow`][geopandas.GeoDataFrame.to_arrow] to convert a GeoPandas `GeoDataFrame` to a GeoArrow table object. This table object is compatible with any GeoArrow-compatible library, including `geoarrow-rust` but also [`geoarrow-pyarrow`](https://geoarrow.org/geoarrow-python/main/pyarrow.html#) and [`lonboard`](https://developmentseed.org/lonboard/latest/).

Use [`GeoDataFrame.from_arrow`][geopandas.GeoDataFrame.from_arrow] to convert a GeoArrow table back to a GeoPandas `GeoDataFrame`. This supports all GeoArrow geometry types _except_ for the new Geometry, GeometryCollection, and Box array types introduced in GeoArrow specification version 0.2.
