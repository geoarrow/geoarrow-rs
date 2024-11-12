# GDAL

GDAL natively supports reading data from any vector driver as GeoArrow data, and natively supports writing data to any vector driver from GeoArrow data.

For reading and writing you have two options:

- You can use [`pyogrio`'s Arrow integration](https://pyogrio.readthedocs.io/en/latest/api.html#arrow-integration) directly
- You can use the [`geoarrow.rust.core.read_pyogrio`][] wrapper.

    This calls `pyogrio` under the hood (and requires that `pyogrio` is installed). The wrapper lives in `geoarrow.rust.core` because it has no dependency on any Rust IO code.
