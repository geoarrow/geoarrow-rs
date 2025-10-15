# Changelog

**This is the changelog for the core Rust library**. There's a [separate changelog](./python/CHANGELOG.md) for the Python bindings, and there will be another for the JS bindings.

## Unreleased

## 0.6.0 - 2025-10-15

- fix(geoarrow-array): Fix persisting CRS when creating `GeometryArray` from `Field` by @kylebarron in https://github.com/geoarrow/geoarrow-rs/pull/1326
- feat(geoarrow-array): Expose public accessors for coordinates from coord buffers by @kylebarron in https://github.com/geoarrow/geoarrow-rs/pull/1332
- New `GeozeroRecordBatchWriter` to allow for an iterative push-based API for writing to geozero-based data sinks.
- perf(geoarrow-array): Improve perf of from_wkb/from_wkt when parsing to WKB/WKT output types https://github.com/geoarrow/geoarrow-rs/pull/1313
- feat(geoarrow-array): WrapArray trait by @kylebarron in https://github.com/geoarrow/geoarrow-rs/pull/1334
- feat: New API for WKB builder to push bytes/str directly by @kylebarron in https://github.com/geoarrow/geoarrow-rs/pull/1354
- test(geoarrow-array): Add test that GeometryArray slicing survives arrow-rs roundtrip by @kylebarron in https://github.com/geoarrow/geoarrow-rs/pull/1371
- feat(geoarrow-array): Implement child array access from GeometryArray #1373
- docs: Improve docs wording for GeoArrowArrayReader by @kylebarron in https://github.com/geoarrow/geoarrow-rs/pull/1358

## 0.5.0 - 2025-08-07

- feat(geoarrow-array): implement GeozeroGeometry trait on WkbView and WktView arrays by @kylebarron in https://github.com/geoarrow/geoarrow-rs/pull/1187
- feat(geoarrow-array): ergonomic default implementations of GeoArrowArray trait by @kylebarron in https://github.com/geoarrow/geoarrow-rs/pull/1188
- feat(geoarrow-array): Implement GeozeroGeometry for RectArray by @kylebarron in https://github.com/geoarrow/geoarrow-rs/pull/1189
- feat(geoarrow-array): Add `GeoArrowArrayReader` trait and `GeoArrowArrayIterator` struct by @kylebarron in https://github.com/geoarrow/geoarrow-rs/pull/1196
- fix(geoarrow-array): inconsistencies between code and comment by @YichiZhang0613 in https://github.com/geoarrow/geoarrow-rs/pull/1206
- feat(geoarrow-array): Pass down arbitrary parameters in `downcast_geoarrow_array` by @kylebarron in https://github.com/geoarrow/geoarrow-rs/pull/1230
- feat(geoarrow-array): add shrink_to_fit methods to builders by @kylebarron in https://github.com/geoarrow/geoarrow-rs/pull/1268
- perf(geoarrow-array): Cast Wkb/Wkt to WkbViewArray/WktViewArray without parsing/re-encoding geometries by @kylebarron in https://github.com/geoarrow/geoarrow-rs/pull/1263
- fix(geoarrow-array): Allow pushing Rect to MultiPolygonBuilder by @kylebarron in https://github.com/geoarrow/geoarrow-rs/pull/1269
- fix(geoarrow-array): Fix `num_bytes` calculations for non-XY dimensions by @kylebarron in https://github.com/geoarrow/geoarrow-rs/pull/1277
- **Breaking** fix(geoarrow-array)!: WkbBuilder should return result when appending geometries by @kylebarron in https://github.com/geoarrow/geoarrow-rs/pull/1271

---

Previous releases were documented in the top-level `CHANGELOG.md` file.
