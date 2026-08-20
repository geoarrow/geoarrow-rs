# Changelog

**This is the changelog for the core Rust library**. There's a [separate changelog](./python/CHANGELOG.md) for the Python bindings, and there will be another for the JS bindings.

## Unreleased

- **Breaking:** the reader returns errors instead of panicking on malformed files; `num_rows` now returns `GeoArrowResult<usize>`.
- Native-encoded columns take their geometry type from the encoding and layout, so an empty or inconsistent `geometry_types` list reads.
- The reader validates the native coordinate struct: Float64 fields x, y, z, m in that order, at the encoding's nesting depth.
- **Breaking:** the geo metadata `bbox` is a typed `GeoParquetBbox` (4, 6, or 8 elements), validated in deserialization.
- Dataset schemas are compared by field name and data type; nullability merges as "nullable in any file".
- The dataset metadata merge is order-independent per column: `geometry_types` union, and columns declared by only some files carry over.
- Covering shape checks moved to the row-filter path; row-group pruning and bounds work for flat top-level coverings.
- New `GeoParquetVersion` enum: `GeoParquetWriterOptions` takes a target version (default 1.1). Native encodings and coverings require 1.1, M or ZM geometries 2.0. `GeoParquetMetadata::known_version` interprets the file's version string.

## 0.7.0 - 2026-01-04

- chore: Bump to arrow 57 by @ddupg in https://github.com/geoarrow/geoarrow-rs/pull/1402

## 0.6.0 - 2025-10-15

- Make `infer_geoarrow_schema` public. #1251

## 0.5.0 - 2025-08-07

No changes.

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
