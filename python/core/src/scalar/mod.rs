pub mod geo_interface;
pub mod repr;

use pyo3::prelude::*;

macro_rules! impl_scalar {
    (
        $(#[$($attrss:meta)*])*
        pub struct $struct_name:ident(pub(crate) $geoarrow_scalar:ty);
    ) => {
        $(#[$($attrss)*])*
        #[pyclass(module = "geoarrow.rust.core._rust")]
        pub struct $struct_name(pub(crate) $geoarrow_scalar);

        impl From<$geoarrow_scalar> for $struct_name {
            fn from(value: $geoarrow_scalar) -> Self {
                Self(value)
            }
        }

        impl From<$struct_name> for $geoarrow_scalar {
            fn from(value: $struct_name) -> Self {
                value.0
            }
        }
    };
}

impl_scalar! {
    /// An immutable Point scalar using GeoArrow's in-memory representation.
    ///
    /// **Note**: for best performance, do as many operations as possible on arrays or chunked
    /// arrays instead of scalars.
    pub struct Point(pub(crate) geoarrow::scalar::OwnedPoint);
}
impl_scalar! {
    /// An immutable LineString scalar using GeoArrow's in-memory representation.
    ///
    /// **Note**: for best performance, do as many operations as possible on arrays or chunked
    /// arrays instead of scalars.
    pub struct LineString(pub(crate) geoarrow::scalar::OwnedLineString<i32>);
}
impl_scalar! {
    /// An immutable Polygon scalar using GeoArrow's in-memory representation.
    ///
    /// **Note**: for best performance, do as many operations as possible on arrays or chunked
    /// arrays instead of scalars.
    pub struct Polygon(pub(crate) geoarrow::scalar::OwnedPolygon<i32>);
}
impl_scalar! {
    /// An immutable MultiPoint scalar using GeoArrow's in-memory representation.
    ///
    /// **Note**: for best performance, do as many operations as possible on arrays or chunked
    /// arrays instead of scalars.
    pub struct MultiPoint(pub(crate) geoarrow::scalar::OwnedMultiPoint<i32>);
}
impl_scalar! {
    /// An immutable MultiLineString scalar using GeoArrow's in-memory representation.
    ///
    /// **Note**: for best performance, do as many operations as possible on arrays or chunked
    /// arrays instead of scalars.
    pub struct MultiLineString(pub(crate) geoarrow::scalar::OwnedMultiLineString<i32>);
}
impl_scalar! {
    /// An immutable MultiPolygon scalar using GeoArrow's in-memory representation.
    ///
    /// **Note**: for best performance, do as many operations as possible on arrays or chunked
    /// arrays instead of scalars.
    pub struct MultiPolygon(pub(crate) geoarrow::scalar::OwnedMultiPolygon<i32>);
}
impl_scalar! {
    /// An immutable Geometry scalar using GeoArrow's in-memory representation.
    ///
    /// **Note**: for best performance, do as many operations as possible on arrays or chunked
    /// arrays instead of scalars.
    pub struct Geometry(pub(crate) geoarrow::scalar::OwnedGeometry<i32>);
}
impl_scalar! {
    /// An immutable GeometryCollection scalar using GeoArrow's in-memory representation.
    ///
    /// **Note**: for best performance, do as many operations as possible on arrays or chunked
    /// arrays instead of scalars.
    pub struct GeometryCollection(pub(crate) geoarrow::scalar::OwnedGeometryCollection<i32>);
}
impl_scalar! {
    /// An immutable WKB-encoded scalar using GeoArrow's in-memory representation.
    ///
    /// **Note**: for best performance, do as many operations as possible on arrays or chunked
    /// arrays instead of scalars.
    pub struct WKB(pub(crate) geoarrow::scalar::OwnedWKB<i32>);
}
impl_scalar! {
    /// An immutable Rect scalar using GeoArrow's in-memory representation.
    ///
    /// **Note**: for best performance, do as many operations as possible on arrays or chunked
    /// arrays instead of scalars.
    pub struct Rect(pub(crate) geoarrow::scalar::OwnedRect);
}
