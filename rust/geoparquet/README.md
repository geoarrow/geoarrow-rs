# geoparquet

Support for reading and writing [GeoParquet][geoparquet repo] files.

The reader and writer APIs are built on top of the [`parquet`] crate, and are designed to be used in conjunction with the upstream [arrow-enabled APIs][parquet::arrow].

See the [crate-level `parquet` documentation][parquet] and the [GeoParquet specification][geoparquet repo] for more high-level details about the GeoParquet format.

[geoparquet repo]: https://github.com/opengeospatial/geoparquet

## Feature flags

This crate provides the following features which may be enabled in your `Cargo.toml`:

- `async`: support `async` APIs for reading and writing GeoParquet

You can enable compression codecs for reading and writing GeoParquet files directly via the upstream `parquet` crate's feature flags.

## Rust version compatibility

This crate is tested with the latest stable version of Rust. We do not currently test against other, older versions of the Rust compiler.
