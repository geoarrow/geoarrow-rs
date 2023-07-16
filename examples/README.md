# Rust Examples

## Usage with GDAL Arrow API

As of GDAL version 3.6, GDAL has included support for [reading data into Arrow
directly](https://gdal.org/development/rfc/rfc86_column_oriented_api.html). The file
[`gdal.rs`](gdal.rs) shows how to use this API to efficiently load data into GeoArrow memory.

Run with

```bash
cargo run --example gdal --features gdal
```
