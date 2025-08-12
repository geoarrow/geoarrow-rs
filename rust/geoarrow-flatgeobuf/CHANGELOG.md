# Changelog

**This is the changelog for the core Rust library**. There's a [separate changelog](./python/CHANGELOG.md) for the Python bindings, and there will be another for the JS bindings.

## Unreleased

- New `FlatGeobufWriter`: an iterative push-based API for writing to FlatGeobuf files.

## 0.5.0 - 2025-08-07

- feat(flatgeobuf): Restore the `FlatGeobuf` reader by @kylebarron in https://github.com/geoarrow/geoarrow-rs/pull/1057
- feat(flatgeobuf): Remove wrappers around Fgb reader objects, simplify API by @kylebarron in https://github.com/geoarrow/geoarrow-rs/pull/1250
- feat(geoarrow-flatgeobuf): Improved schema inference & expose more from header by @kylebarron in https://github.com/geoarrow/geoarrow-rs/pull/1260
- chore(geoarrow-flatgeobuf): update flatgeobuf dep to 5.0 by @kylebarron in https://github.com/geoarrow/geoarrow-rs/pull/1261
- test(geoarrow-flatgeobuf): More tests for FlatGeobuf writer by @kylebarron in https://github.com/geoarrow/geoarrow-rs/pull/1285
- fix(geoarrow-flatgeobuf): Fix calculation of `num_rows_remaining` to allocate record batch builders correctly by @kylebarron in https://github.com/geoarrow/geoarrow-rs/pull/1286
- refactor(geoarrow-flatgeobuf): Improved API for passing in or inferring schema by @kylebarron in https://github.com/geoarrow/geoarrow-rs/pull/1272

---

Previous releases were documented in the top-level `CHANGELOG.md` file.
