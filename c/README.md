# geoarrow-c-api

A C ABI over geoarrow-rs, for callers that cannot link Rust directly.

Arrays cross the boundary through the [Arrow C Data Interface], so geospatial
data is neither copied nor reserialized. `cargo build` builds `cdylib` and
`staticlib` artifacts.

## The header

`geoarrow_rs.h` is committed. The compute entry points are macro-generated, so
regeneration runs cbindgen with macro expansion, which compiles through a
nightly toolchain:

```sh
cd c && RUSTUP_TOOLCHAIN=nightly cbindgen --output geoarrow_rs.h
```

`tests/header_parity.rs` fails when the header, the sources and the js
bindings drift apart.

## Relationship to geoarrow-c

The type vocabulary is shared with [geoarrow-c]: `GeoArrowError`,
`GeoArrowErrorCode`, `GEOARROW_OK`, `GeoArrowType`, `GeoArrowCoordType`,
`GeoArrowDimensions` and `GeoArrowEdgeType` keep geoarrow-c's names and values,
so one set of constants works with both libraries. Functions carry a
`GeoArrowRs` prefix instead, so both libraries can be linked into one program.
The Arrow C Data Interface structs are guarded with `ARROW_C_DATA_INTERFACE`,
so this header coexists with `arrow/c/abi.h` and friends in one translation
unit; the shared vocabulary types are plain redefinitions, so `geoarrow.h` and
`geoarrow_rs.h` still belong in separate translation units.

Two families of `GeoArrowType` value have no geoarrow-c counterpart and follow
geoarrow-c's own encoding, `(coord_type - 1) * 10000 + (dimensions - 1) * 1000
+ geometry_type`: the `GEOMETRYCOLLECTION` values reuse `GeoArrowGeometryType`'s
slot 7, and `geoarrow.geometry` takes 991, next to `BOX` at 990.

## Error handling

Functions return an errno-compatible `GeoArrowErrorCode` and write detail into a
caller-allocated `GeoArrowError`, which may be null.

```c
struct GeoArrowError error;
if (GeoArrowRsCentroid(&in_array, &in_schema, GEOARROW_COORD_TYPE_SEPARATE,
                       &out_array, &out_schema, &error) != GEOARROW_OK) {
  fprintf(stderr, "%s\n", error.message);
}
```

## Ownership

Input array and schema pairs are consumed: the bytes are moved out and the
caller's structs are zeroed, so the caller must not release them afterwards.
Output pairs are written into caller-allocated structs and released with
`GeoArrowRsArrayRelease` and `GeoArrowRsSchemaRelease`.

`GeoArrowRsGeoParquetReaderNext` reports the end of the stream the way the Arrow
C Stream Interface does: it returns `GEOARROW_OK` and leaves
`out_array->release` null.

[Arrow C Data Interface]: https://arrow.apache.org/docs/format/CDataInterface.html
[geoarrow-c]: https://geoarrow.org/geoarrow-c
