# Changelog

## Unreleased

## [0.4.1] - 2025-07-17

- Added `path` property to `GeoParquetFile` class so information about each fragment of a `GeoParquetDataset` can be associated back to the original file.

## [0.4.0] - 2025-07-01

This release contains the Python bindings for more or less a **full rewrite** of the GeoArrow Rust library.

### New Features :magic_wand:

- Full support for the GeoArrow 0.2 specification, including all geometry array types and dimensions.
- See documentation for `GeoArray`, `GeoChunkedArray`, `GeoScalar`, and `GeometryType` for more details.
- Support for reading and writing GeoParquet 1.1 files, including spatial filtering, including support for reading from remote files.

## [0.3.0] - 2024-09-07

### New Features :magic_wand:

- Remove geometry class specializations. Instead of `PointArray`, `LineStringArray`, etc, there's now just `GeometryArray`, `ChunkedGeometryArray`, and `Geometry` (a scalar).
- Remove GeoTable class, in favor of external, generic arrow Table implementations, such as `arro3.core.Table`.
- Move to slimmer, functional API. No more geometry methods on classes.
- Don't materialize input data when writing to a file.
- New `GeometryType` class for understanding the geometry type of an array or chunked array.
- Split Python code into three modules: `geoarrow-rust-core`, `geoarrow-rust-compute` and `geoarrow-rust-io`.
- Support for Pyodide Python environment
- Support Python file objects for reading and writing GeoParquet

### Bug fixes :bug:

- Fix array indexing with negative integers by @kylebarron in https://github.com/geoarrow/geoarrow-rs/pull/724

## [0.2.0] - 2024-03-22

### New! :sparkles:

#### New I/O support for reading and writing to geospatial formats

- [Asynchronous FlatGeobuf reader](https://geoarrow.org/geoarrow-rs/python/v0.2.0/api/core/io/#geoarrow.rust.core.read_flatgeobuf_async). This also supports passing a spatial filter.
- [Initial support for reading from PostGIS](https://geoarrow.org/geoarrow-rs/python/v0.2.0/api/core/io/#geoarrow.rust.core.read_postgis). Note that not all Postgres data types are supported yet. Create an issue if your query fails.
- [Reading](https://geoarrow.org/geoarrow-rs/python/v0.2.0/api/core/io/#geoarrow.rust.core.read_geojson_lines) and [writing](https://geoarrow.org/geoarrow-rs/python/v0.2.0/api/core/io/#geoarrow.rust.core.write_geojson_lines) [newline-delimited GeoJSON](https://stevage.github.io/ndgeojson/).
- [Pyogrio integration](https://geoarrow.org/geoarrow-rs/python/v0.2.0/api/core/io/#geoarrow.rust.core.read_pyogrio) for reading from OGR/GDAL.
- [Asynchronous GeoParquet reader](https://geoarrow.org/geoarrow-rs/python/v0.2.0/api/core/io/#geoarrow.rust.core.read_parquet_async) for reading from remote files. By @weiji14 in https://github.com/geoarrow/geoarrow-rs/pull/493
- Also new support for writing GeoParquet files.
- Most I/O readers and writers support Python file-like objects (in binary mode).
- Support for [reading](https://geoarrow.org/geoarrow-rs/python/v0.2.0/api/core/io/#geoarrow.rust.core.read_ipc) and [writing](https://geoarrow.org/geoarrow-rs/python/v0.2.0/api/core/io/#geoarrow.rust.core.write_ipc) Arrow IPC files.

#### Better interoperability with the Python geospatial ecosystem.

- [Import from](https://geoarrow.org/geoarrow-rs/python/v0.2.0/api/core/interop/#geoarrow.rust.core.from_geopandas) and [export to](https://geoarrow.org/geoarrow-rs/python/v0.2.0/api/core/interop/#geoarrow.rust.core.to_geopandas) GeoPandas GeoDataFrames. Refer to the [GeoPandas interoperability documentation](https://geoarrow.org/geoarrow-rs/python/v0.2.0/ecosystem/geopandas/).
- [Import from](https://geoarrow.org/geoarrow-rs/python/v0.2.0/api/core/interop/#geoarrow.rust.core.from_shapely) and [export to](https://geoarrow.org/geoarrow-rs/python/v0.2.0/api/core/interop/#geoarrow.rust.core.to_shapely) Shapely arrays. Refer to the [Shapely interoperability documentation](https://geoarrow.org/geoarrow-rs/python/v0.2.0/ecosystem/shapely/).
- [Better integration with Lonboard](https://geoarrow.org/geoarrow-rs/python/v0.2.0/ecosystem/lonboard/) for fast visualization in Jupyter.
- All scalars, arrays, chunked arrays, and table objects implement [`__geo_interface__`](https://gist.github.com/sgillies/2217756) for interoperability with existing tools.
- Numpy interoperability for float array output from algorithms. Pass any `Float64Array` or `BooleanArray` to `numpy.asarray`. You can also pass a numpy array as vectorized input into a function like [`line_interpolate_point`](https://geoarrow.org/geoarrow-rs/python/v0.2.0/api/core/functions/#geoarrow.rust.core.line_interpolate_point).

#### New algorithms!

- [Explode a Table](https://geoarrow.org/geoarrow-rs/python/v0.2.0/api/core/functions/#geoarrow.rust.core.explode) where each multi-geometry expands into multiple rows of single geometries.
- [`total_bounds`](https://geoarrow.org/geoarrow-rs/python/v0.2.0/api/core/functions/#geoarrow.rust.core.total_bounds)
- Unified [`area` function](https://geoarrow.org/geoarrow-rs/python/v0.2.0/api/core/functions/#geoarrow.rust.core.area) for planar and geodesic area.
- Unified [`simplify` function](https://geoarrow.org/geoarrow-rs/python/v0.2.0/api/core/functions/#geoarrow.rust.core.simplify) for multiple simplification methods. Also new support for [topology-preserving simplification](https://geoarrow.org/geoarrow-rs/python/v0.2.0/api/core/enums/#geoarrow.rust.core.enums.SimplifyMethod.VW_Preserve).
- Unified [`length` function](https://geoarrow.org/geoarrow-rs/python/v0.2.0/api/core/functions/#geoarrow.rust.core.length) for euclidean and geodesic length calculations.
- [`frechet_distance`](https://geoarrow.org/geoarrow-rs/python/v0.2.0/api/core/functions/#geoarrow.rust.core.frechet_distance) for LineString similarity.
- [`affine_transform`](https://geoarrow.org/geoarrow-rs/python/v0.2.0/api/core/functions/#geoarrow.rust.core.affine_transform), which integrates with the Python [`affine`](https://github.com/rasterio/affine) library.
- [`line_interpolate_point`](https://geoarrow.org/geoarrow-rs/python/v0.2.0/api/core/functions/#geoarrow.rust.core.line_interpolate_point) and [`line_locate_point`](https://geoarrow.org/geoarrow-rs/python/v0.2.0/api/core/functions/#geoarrow.rust.core.line_locate_point) for linear referencing.
- [`polylabel`](https://geoarrow.org/geoarrow-rs/python/v0.2.0/api/core/functions/#geoarrow.rust.core.polylabel) for polygon labeling.

#### Improved display of Python objects:

- Scalars now display as SVG geometries inside Jupyter environments.
- Tables, arrays, and chunked arrays implement `__repr__` so that you can inspect data easily.

#### Improved usability:

- [`PointArray.from_xy`](https://geoarrow.org/geoarrow-rs/python/v0.2.0/api/core/geometry/array/#geoarrow.rust.core.PointArray.from_xy) to simplify creating a point array from numpy arrays of coordinates.
- Index into arrays and chunked arrays with square brackets. E.g. `point_array[0]` will access the first point in the array. Negative indexing is also supported, so `point_array[-1]` will get the last item in the array.
- New [top-level docs website](https://geoarrow.org/geoarrow-rs/).

## New Contributors

- @Robinlovelace made their first contribution in https://github.com/geoarrow/geoarrow-rs/pull/484
- @weiji14 made their first contribution in https://github.com/geoarrow/geoarrow-rs/pull/493

**Full Changelog**: https://github.com/geoarrow/geoarrow-rs/compare/py-v0.1.0...py-v0.2.0

## [0.1.0] - 2024-01-08

- Initial public release.
