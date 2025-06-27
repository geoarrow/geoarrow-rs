# GeoArrow Rust crates

[![GitHub Workflow Status (CI)](https://img.shields.io/github/actions/workflow/status/geoarrow/geoarrow-rs/ci.yml?branch=main)](https://github.com/geoarrow/geoarrow-rs/actions/workflows/ci.yml)
![Crates.io](https://img.shields.io/crates/l/geoarrow)

A Rust implementation of the [GeoArrow](https://github.com/geoarrow/geoarrow) specification.

<!-- and bindings to [GeoRust algorithms](https://github.com/georust/geo) for efficient spatial operations on GeoArrow memory. -->

## Crates

The `geoarrow-rs` repo is currently undergoing a large refactor from a single crate (`geoarrow`) to a monorepo of smaller crates, each with a more well-defined scope. As of May 2025, avoid using the `geoarrow` crate and instead use the newer crates with a smaller scope, like `geoarrow-array` and `geoarrow-schema`.

| Name                  | Description                                                          | Stability                              | Version                                                                                                           | Docs                                                                                                               |
| --------------------- | -------------------------------------------------------------------- | -------------------------------------- | ----------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| `geoarrow-array`      | GeoArrow array definitions.                                          | Pretty stable                          | [![Crates.io](https://img.shields.io/crates/v/geoarrow-array)](https://crates.io/crates/geoarrow-array)           | [![docs.rs](https://img.shields.io/docsrs/geoarrow-array?label=docs.rs)](https://docs.rs/geoarrow-array)           |
| `geoarrow-cast`       | Functions for converting from one GeoArrow geometry type to another. | Pretty stable                          | [![Crates.io](https://img.shields.io/crates/v/geoarrow-cast)](https://crates.io/crates/geoarrow-cast)             | [![docs.rs](https://img.shields.io/docsrs/geoarrow-cast?label=docs.rs)](https://docs.rs/geoarrow-cast)             |
| `geoarrow-flatgeobuf` | Reader and writer for FlatGeobuf files to GeoArrow memory.           | Somewhat stable                        | [![Crates.io](https://img.shields.io/crates/v/geoarrow-flatgeobuf)](https://crates.io/crates/geoarrow-flatgeobuf) | [![docs.rs](https://img.shields.io/docsrs/geoarrow-flatgeobuf?label=docs.rs)](https://docs.rs/geoarrow-flatgeobuf) |
| `geoarrow-schema`     | GeoArrow geometry type and metadata definitions.                     | Pretty stable                          | [![Crates.io](https://img.shields.io/crates/v/geoarrow-schema)](https://crates.io/crates/geoarrow-schema)         | [![docs.rs](https://img.shields.io/docsrs/geoarrow-schema?label=docs.rs)](https://docs.rs/geoarrow-schema)         |
| `geoparquet`          | GeoParquet reader and writer.                                        | Unstable (a refactor is expected soon) | [![Crates.io](https://img.shields.io/crates/v/geoparquet)](https://crates.io/crates/geoparquet)                   | [![docs.rs](https://img.shields.io/docsrs/geoparquet?label=docs.rs)](https://docs.rs/geoparquet)                   |

## Versioning

These crates may diverge in versioning to allow for some sub-crates to receive breaking changes while not forcing a breaking version change to all crates. However, all crates will receive a new breaking version at least every 3 months, as the upstream `arrow-rs` crates currently publish a breaking version every 3 months.
