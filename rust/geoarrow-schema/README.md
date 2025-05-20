# geoarrow-schema

GeoArrow geometry type and metadata definitions.

All geometry type definitions, such as
[`PointType`][crate::PointType], [`GeometryType`][crate::GeometryType], or
[`WkbType`][crate::WkbType] implement the upstream [`ExtensionType`][arrow_schema::extension::ExtensionType] trait.

Instances of type definitions are included within the variants on the
GeoArrowType enum.
