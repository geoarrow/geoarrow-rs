Support for reading and writing [GeoParquet][geoparquet repo] files.

The reader and writer APIs are built on top of the [`parquet`] crate, and are designed to behave similarly to their [arrow-enabled APIs][parquet::arrow].

See the [crate-level parquet documentation][parquet] and the [GeoParquet specification][geoparquet repo] for more details.

[geoparquet repo]: https://github.com/opengeospatial/geoparquet

## Feature flags

This crate provides the following features which may be enabled in your `Cargo.toml`:

- `async`: support `async` APIs for reading and writing GeoParquet
- `compression`: turn on all compression codecs supported by `parquet`.

Alternatively, you can enable compression algorithms individually:

- `brotli` - support for Parquet using `brotli` compression
- `flate2` - support for Parquet using `gzip` compression
- `lz4` - support for Parquet using `lz4` compression
- `zstd` - support for Parquet using `zstd` compression
- `snap` - support for Parquet using `snappy` compression

## Rust version compatibility

This crate is tested with the latest stable version of Rust. We do not currently test against other, older versions of the Rust compiler.

