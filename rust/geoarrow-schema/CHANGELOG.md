# Changelog

**This is the changelog for the core Rust library**. There's a [separate changelog](./python/CHANGELOG.md) for the Python bindings, and there will be another for the JS bindings.

## Unreleased

## 0.6.0 - 2025-10-15

- feat(geoarrow-schema): add `RectType` as alias for `BoxType` by @kylebarron in https://github.com/geoarrow/geoarrow-rs/pull/1310
- feat: `GeometryTypeId` trait to infer GeoArrow type id for a type #1372

## 0.5.0 - 2025-08-07

- **Breaking**: Use GeoArrowError in TryFrom from geo_traits::Dimension by @kylebarron in https://github.com/geoarrow/geoarrow-rs/pull/1266
- Check inferred dimension against list size for interleaved point input by @kylebarron in https://github.com/geoarrow/geoarrow-rs/pull/1267
- **Breaking**: Better distinction of creating GeoArrowType depending on extension metadata by @kylebarron in https://github.com/geoarrow/geoarrow-rs/pull/1275

---

Previous releases were documented in the top-level `CHANGELOG.md` file.
