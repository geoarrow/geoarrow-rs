# GDAL

GDAL natively supports reading data from any vector driver as GeoArrow data, and natively supports writing data to any vector driver from GeoArrow data.

For reading and writing, use [`pyogrio`'s Arrow integration](https://pyogrio.readthedocs.io/en/latest/api.html#arrow-integration) directly, which supports WKB-encoded GeoArrow data.
