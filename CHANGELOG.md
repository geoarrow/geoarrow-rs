# Changelog

**Changelogs have been moved to per-crate changelogs**.

## 0.4.0 (`geoparquet` crate) - 2025-07-08

New standalone `geoparquet` crate!

### New Features :magic_wand:

- GeoParquet reader refactor to avoid making duplicate wrappers of upstream structs #1089
  - Removes wrapper structs for builders. So we no longer have a `GeoParquetRecordBatchReaderBuilder` or a `GeoParquetRecordBatchStreamBuilder`. Users will use upstream `parquet` sync/async `Builder` structs directly. Means a significantly lower maintenance overhead here, and we don't need to do anything to support new upstream functionality.
  - Adds `GeoParquetReaderBuilder` **trait** that extends the upstream [`ArrowReaderBuilder`](https://docs.rs/parquet/latest/parquet/arrow/arrow_reader/struct.ArrowReaderBuilder.html). This allows users to implement a spatial [`RowFilter`](https://docs.rs/parquet/latest/parquet/arrow/arrow_reader/struct.RowFilter.html) directly on an upstream builder instance. It also allows low-level access, so that if a user wanted a spatial filter _plus_ something else, that would be doable.
  - Allows user to choose when the WKB column should be parsed to a native GeoArrow type or not in the `geoarrow_schema` method. (the `parse_to_native` parameter)
  - **Keeps wrapper structs for readers**. So we still have a `GeoParquetRecordBatchReader` and a `GeoParquetRecordBatchStream`, but these are _very lightweight_ wrappers.

      The benefit of having these wrapper structs is that we can ensure GeoArrow metadata is always applied onto the emitted `RecordBatch`es.
- Refactor of GeoParquetReaderMetadata and GeoParquetDatasetMetadata #1172
  - Remove `Option` for the geospatial metadata of each `GeoParquetReaderMetadata` and `GeoParquetDatasetMetadata`. We assume that if the user is using the `geoparquet` crate, all files will have valid GeoParquet metadata.
- Remove GeoParquet writer wrappers and just expose encoder #1214
  - Similar to how we refactored the read side (https://github.com/geoarrow/geoarrow-rs/pull/1089, https://github.com/geoarrow/geoarrow-rs/pull/1172), this PR changes the writing APIs of the `geoparquet` crate to only provide _additive_ APIs to the upstream `parquet` crate. So this is now intended to be used _in conjunction with_ the upstream `parquet` crate, not exclusively of it.
- Add support for writing covering column to GeoParquet #1216
  - Add support for generating the a bounding box covering column while writing data.
  - Reconfigure writer properties generation. This takes inspiration from the upstream `parquet` properties generation, and has both _column_ specific properties and _default_ properties. So you can set the encoding for _all_ columns but you can also override the encodings for specific columns.
- Allow passing primary column to parquet writer #1159
- Correctly set geometry types in GeoParquet metadata when writing #1218

## [0.4.0] - 2025-05-28

This release contains more or less a **full rewrite** of the GeoArrow Rust library.

> The `geoarrow-rs` project is about 3 years old. Early prototyping started inside the [`geopolars`](https://github.com/geopolars/geopolars) repo before deciding I needed something more general (not tied to Polars) and creating first [`geopolars/geoarrow`](https://github.com/geopolars/geoarrow) and then lastly [`geoarrow/geoarrow-rs`](https://github.com/geoarrow/geoarrow-rs).
>
> However despite its age, the `geoarrow` crate suffers from a couple issues. For one, it took a while to figure out the right abstractions. And learning Rust at the same time didn't help. The `geoarrow` crate is also too monolithic. Some parts of `geoarrow` are production ready, but there's decidedly a _lot_ of code in `geoarrow` that is _not_ production ready. And it's very much not clear which parts of `geoarrow` are production ready or not.
>
> But I think `geoarrow-rs` has potential to form part of the core geospatial data engineering stack in Rust. And as part of that, we need a better delineation of which parts of the code are stable or not. As such, the `geoarrow-rs` repo is **currently ongoing a large refactor from a single crate to a monorepo of smaller crates, each with a more well-defined scope**.
>
> As of May 2025, avoid using the `geoarrow` crate and instead use the newer crates with a smaller scope, like `geoarrow-array`.

### New Features :magic_wand:

- Full support for the GeoArrow 0.2 specification, including all geometry array types and dimensions. See documentation in `geoarrow-array`.
- Native support for the upstream [Arrow `ExtensionType`](https://github.com/apache/arrow-rs/pull/5822/) concept introduced in arrow `54.2`. See documentation in `geoarrow-schema`.

### Removed functionality :wrench:

A significant amount of code from the previous `geoarrow` crate is not currently available in the refactored `geoarrow-*` crates.

Some of this is intentional. Similar to the upstream `arrow` crate, we no longer export a `ChunkedArray` concept, suggesting the user to use streaming concepts like iterators where possible.

Other functionality like format readers and writers is intended to be restored once stable, but additional work needs to take place before there's enough confidence in that code.

## [0.3.0] - 2024-09-07

### New Features :magic_wand:

- Preliminary support for 3D (XYZ) geometries
- Support for reading and writing GeoParquet 1.1
  - Support for reading and writing GeoArrow (native) geometry encoding
  - Support for reading with spatial filtering
- Both synchronous and asynchronous readers for GeoParquet. Readers will stream a RecordBatch at a time by default.
- Accept `RecordBatchReader` as input to all
- New support for `geoarrow.box` array (equivalent to `Vec<Option<geo::Rect>>`). `RectArray` is now laid out as a `StructArray` internally instead of a `FixedSizeListArray`.
- Improved documentation

### Performance Improvements 🏎️

- Remove `Cow` around scalar buffers by @kylebarron in https://github.com/geoarrow/geoarrow-rs/pull/720

### Bug fixes :bug:

- Don't serialize empty array metadata by @kylebarron in https://github.com/geoarrow/geoarrow-rs/pull/678
- Fixed `MixedGeometryArray` handling. Exported Arrow `UnionArrays` always have same data layout.
- Support MapArrays when exporting to geozero by @kylebarron in https://github.com/geoarrow/geoarrow-rs/pull/721

## New Contributors

- @H-Plus-Time made their first contribution in https://github.com/geoarrow/geoarrow-rs/pull/607
- @gadomski made their first contribution in https://github.com/geoarrow/geoarrow-rs/pull/640

**Full Changelog**: https://github.com/geoarrow/geoarrow-rs/compare/rust-v0.2.0...rust-v0.3.0

## [0.3.0-beta.2] - 2024-08-23

### Added

- Indexed geometry arrays ([#443](https://github.com/geoarrow/geoarrow-rs/pull/443))
- Parse row group stats out of GeoParquet ([#571](https://github.com/geoarrow/geoarrow-rs/pull/571))
- Writing GeoParquet with GeoArrow encoding ([#583](https://github.com/geoarrow/geoarrow-rs/pull/583))
- Async GeoParquet writing ([#587](https://github.com/geoarrow/geoarrow-rs/pull/587))
- Bounding box queries on GeoParquet files ([#590](https://github.com/geoarrow/geoarrow-rs/pull/590))
- More datatypes to GeozeroDatasource implementation ([#619](https://github.com/geoarrow/geoarrow-rs/pull/619))
- Support for Z, M, and ZM data ([#663](https://github.com/geoarrow/geoarrow-rs/pull/663))
- Some more documentation ([#696](https://github.com/geoarrow/geoarrow-rs/pull/696))

### Changed

- Bump msrv to 1.80 ([#702](https://github.com/geoarrow/geoarrow-rs/pull/702))

## [0.2.0] - 2024-03-23

### New! :sparkles:

#### New I/O support for reading and writing to geospatial formats

- Asynchronous FlatGeobuf reader. This also supports passing a spatial filter.
- Initial support for reading from PostGIS. Note that not all Postgres data types are supported yet. Create an issue if your query fails.
- Reading and writing [newline-delimited GeoJSON](https://stevage.github.io/ndgeojson/).
- Asynchronous GeoParquet reader for reading from remote files. By @weiji14 in <https://github.com/geoarrow/geoarrow-rs/pull/493>
- Also new support for writing GeoParquet files.
- Support for reading and writing Arrow IPC files.

#### New algorithms!

- Explode a Table where each multi-geometry expands into multiple rows of single geometries.
- total_bounds
- `frechet_distance` for LineString similarity.
- `line_interpolate_point` and `line_locate_point` for linear referencing.
- `polylabel` for polygon labeling.

#### Improved usability

- New [top-level docs website](https://geoarrow.org/geoarrow-rs/).

## New Contributors

- @Robinlovelace made their first contribution in <https://github.com/geoarrow/geoarrow-rs/pull/484>
- @weiji14 made their first contribution in <https://github.com/geoarrow/geoarrow-rs/pull/493>

**Full Changelog**: <https://github.com/geoarrow/geoarrow-rs/compare/rust-v0.1.0...rust-v0.2.0>

## [0.1.0] - 2024-01-21

- Initial public release.

[0.3.0-beta.2]: https://github.com/geoarrow/geoarrow-rs/compare/rust-v0.2.0...rust-v0.3.0-beta.2
[0.2.0]: https://github.com/geoarrow/geoarrow-rs/compare/rust-v0.1.0...rust-v0.2.0
[0.1.0]: https://github.com/geoarrow/geoarrow-rs/releases/tag/rust-v0.1.0
