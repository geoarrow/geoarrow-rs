# GDAL

GDAL natively supports reading data from any vector driver as GeoArrow data, and natively supports writing data to any vector driver from GeoArrow data.

For reading and writing, use [`pyogrio`'s Arrow integration](https://pyogrio.readthedocs.io/en/latest/api.html#arrow-integration) directly, which supports WKB-encoded GeoArrow data.

This output table can be used with any GeoArrow-compatible library, including `geoarrow-rust` but also [`geoarrow-pyarrow`](https://geoarrow.org/geoarrow-python/main/pyarrow.html#) and [`lonboard`](https://developmentseed.org/lonboard/latest/).
