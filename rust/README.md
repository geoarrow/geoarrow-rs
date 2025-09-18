# GeoArrow Rust crates

[![GitHub Workflow Status (CI)](https://img.shields.io/github/actions/workflow/status/geoarrow/geoarrow-rs/ci.yml?branch=main)](https://github.com/geoarrow/geoarrow-rs/actions/workflows/ci.yml)
![Crates.io](https://img.shields.io/crates/l/geoarrow)

A Rust implementation of the [GeoArrow](https://github.com/geoarrow/geoarrow) specification.

## Crates

| Name                  | Description                                                                                         | Version                                                                                                           | Docs                                                                                                               |
| --------------------- | --------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| `geoarrow`            | Amalgam crate which re-exports items from `geoarrow-array`, `geoarrow-cast`, and `geoarrow-schema`. | [![Crates.io](https://img.shields.io/crates/v/geoarrow)](https://crates.io/crates/geoarrow)                       | [![docs.rs](https://img.shields.io/docsrs/geoarrow?label=docs.rs)](https://docs.rs/geoarrow)                       |
| `geoarrow-array`      | GeoArrow array definitions.                                                                         | [![Crates.io](https://img.shields.io/crates/v/geoarrow-array)](https://crates.io/crates/geoarrow-array)           | [![docs.rs](https://img.shields.io/docsrs/geoarrow-array?label=docs.rs)](https://docs.rs/geoarrow-array)           |
| `geoarrow-cast`       | Functions for converting from one GeoArrow geometry type to another.                                | [![Crates.io](https://img.shields.io/crates/v/geoarrow-cast)](https://crates.io/crates/geoarrow-cast)             | [![docs.rs](https://img.shields.io/docsrs/geoarrow-cast?label=docs.rs)](https://docs.rs/geoarrow-cast)             |
| `geoarrow-flatgeobuf` | Reader and writer for FlatGeobuf files to GeoArrow memory.                                          | [![Crates.io](https://img.shields.io/crates/v/geoarrow-flatgeobuf)](https://crates.io/crates/geoarrow-flatgeobuf) | [![docs.rs](https://img.shields.io/docsrs/geoarrow-flatgeobuf?label=docs.rs)](https://docs.rs/geoarrow-flatgeobuf) |
| `geoarrow-schema`     | GeoArrow geometry type and metadata definitions.                                                    | [![Crates.io](https://img.shields.io/crates/v/geoarrow-schema)](https://crates.io/crates/geoarrow-schema)         | [![docs.rs](https://img.shields.io/docsrs/geoarrow-schema?label=docs.rs)](https://docs.rs/geoarrow-schema)         |
| `geoparquet`          | GeoParquet reader and writer.                                                                       | [![Crates.io](https://img.shields.io/crates/v/geoparquet)](https://crates.io/crates/geoparquet)                   | [![docs.rs](https://img.shields.io/docsrs/geoparquet?label=docs.rs)](https://docs.rs/geoparquet)                   |

Additionally we are working on a few other crates that are not yet distributed on crates.io:

- `geoarrow-geo`: Integration with `geo` crate for spatial algorithms.
- `geoarrow-geos`: Integration with `geos` crate for spatial algorithms.

## Versioning

These crates may possibly diverge in versioning in the future to allow for some sub-crates to receive breaking changes while not forcing a breaking version change to all crates. However, all crates will receive a new breaking version _at least_ every 3 months, as the upstream `arrow` crates currently publish a breaking version every 3 months.

## Version compatibility

| geoarrow | arrow-rs |
| -------- | -------- |
| 0.4.x    | 55       |
| 0.5.x    | 56       |
