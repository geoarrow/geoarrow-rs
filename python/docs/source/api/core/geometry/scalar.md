# Geometry Scalars

The classes on this page represent individual geometry scalars.

Note that operations on arrays will be more performant than operations on scalars. Use arrays and chunked arrays where possible.

- [`Point`](#geoarrow.rust.core.Point)
- [`LineString`](#geoarrow.rust.core.LineString)
- [`Polygon`](#geoarrow.rust.core.Polygon)
- [`MultiPoint`](#geoarrow.rust.core.MultiPoint)
- [`MultiLineString`](#geoarrow.rust.core.MultiLineString)
- [`MultiPolygon`](#geoarrow.rust.core.MultiPolygon)
- [`MixedGeometry`](#geoarrow.rust.core.MixedGeometry)
- [`GeometryCollection`](#geoarrow.rust.core.GeometryCollection)
- [`WKB`](#geoarrow.rust.core.WKB)
- [`Rect`](#geoarrow.rust.core.Rect)

::: geoarrow.rust.core
    options:
      filters:
        - "!^_"
        - "^__arrow"
      members:
        - Point
        - LineString
        - Polygon
        - MultiPoint
        - MultiLineString
        - MultiPolygon
        - MixedGeometry
        - GeometryCollection
        - WKB
        - Rect
