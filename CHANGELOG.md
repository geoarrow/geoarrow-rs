# Changelog

**This is the changelog for the core Rust library**. There's a [separate changelog](./python/CHANGELOG.md) for the Python bindings, and there will be another for the JS bindings.

## Unreleased

### Breaking changes

- `GeometryArrayTrait` renamed to `NativeArray`.
- `GeometryArrayRef` renamed to `NativeArrayRef`.
- `GeometryArrayTrait` renamed to `NativeScalar`.
- `GeometryArrayDyn` renamed to `NativeArrayDyn`.
- `AsGeometryArray` renamed to `AsNativeArray`.
- `AsChunkedGeometryArray` renamed to `AsChunkedNativeArray`.
- `ChunkedGeometryArrayTrait` renamed to `ChunkedNativeArray`.

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
