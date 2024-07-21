pub mod geo_interface;
pub mod getitem;
pub mod repr;

use crate::error::PyGeoArrowResult;
use crate::ffi::from_python::input::PyScalarBuffer;
use arrow::datatypes::Float64Type;
use geoarrow::array::SeparatedCoordBuffer;

use pyo3::prelude::*;
use pyo3::types::PyType;

// use std::sync::Arc;
// #[pyclass(module = "geoarrow.rust.core._rust")]
// pub struct GeometryArray(pub(crate) Arc<dyn geoarrow::trait_::GeometryArrayTrait>);

// impl From<Arc<dyn geoarrow::trait_::GeometryArrayTrait>> for GeometryArray {
//     fn from(value: Arc<dyn geoarrow::trait_::GeometryArrayTrait>) -> Self {
//         Self(value)
//     }
// }

// impl From<GeometryArray> for Arc<dyn geoarrow::trait_::GeometryArrayTrait> {
//     fn from(value: GeometryArray) -> Self {
//         value.0
//     }
// }

macro_rules! impl_array {
    (
        $(#[$($attrss:meta)*])*
        pub struct $struct_name:ident(pub(crate) $geoarrow_arr:ty);
    ) => {
        $(#[$($attrss)*])*
        #[pyclass(module = "geoarrow.rust.core._rust")]
        pub struct $struct_name(pub(crate) $geoarrow_arr);

        impl From<$geoarrow_arr> for $struct_name {
            fn from(value: $geoarrow_arr) -> Self {
                Self(value)
            }
        }

        impl From<$struct_name> for $geoarrow_arr {
            fn from(value: $struct_name) -> Self {
                value.0
            }
        }
    };
}

impl_array! {
    /// An immutable array of Point geometries using GeoArrow's in-memory representation.
    pub struct PointArray(pub(crate) geoarrow::array::PointArray<2>);
}
impl_array! {
    /// An immutable array of LineString geometries using GeoArrow's in-memory representation.
    pub struct LineStringArray(pub(crate) geoarrow::array::LineStringArray<i32, 2>);
}
impl_array! {
    /// An immutable array of Polygon geometries using GeoArrow's in-memory representation.
    pub struct PolygonArray(pub(crate) geoarrow::array::PolygonArray<i32, 2>);
}
impl_array! {
    /// An immutable array of MultiPoint geometries using GeoArrow's in-memory representation.
    pub struct MultiPointArray(pub(crate) geoarrow::array::MultiPointArray<i32, 2>);
}
impl_array! {
    /// An immutable array of MultiLineString geometries using GeoArrow's in-memory representation.
    pub struct MultiLineStringArray(pub(crate) geoarrow::array::MultiLineStringArray<i32, 2>);
}
impl_array! {
    /// An immutable array of MultiPolygon geometries using GeoArrow's in-memory representation.
    pub struct MultiPolygonArray(pub(crate) geoarrow::array::MultiPolygonArray<i32, 2>);
}
impl_array! {
    /// An immutable array of Geometry geometries using GeoArrow's in-memory representation.
    pub struct MixedGeometryArray(pub(crate) geoarrow::array::MixedGeometryArray<i32, 2>);
}
impl_array! {
    /// An immutable array of GeometryCollection geometries using GeoArrow's in-memory
    /// representation.
    pub struct GeometryCollectionArray(pub(crate) geoarrow::array::GeometryCollectionArray<i32, 2>);
}
impl_array! {
    /// An immutable array of WKB-encoded geometries using GeoArrow's in-memory representation.
    pub struct WKBArray(pub(crate) geoarrow::array::WKBArray<i32>);
}
impl_array! {
    /// An immutable array of Rect geometries using GeoArrow's in-memory representation.
    pub struct RectArray(pub(crate) geoarrow::array::RectArray);
}

#[pymethods]
impl WKBArray {
    fn to_point_array(&self) -> PyGeoArrowResult<PointArray> {
        Ok(PointArray(self.0.clone().try_into()?))
    }

    fn to_line_string_array(&self) -> PyGeoArrowResult<LineStringArray> {
        Ok(LineStringArray(self.0.clone().try_into()?))
    }

    fn to_polygon_array(&self) -> PyGeoArrowResult<PolygonArray> {
        Ok(PolygonArray(self.0.clone().try_into()?))
    }

    fn to_multi_point_array(&self) -> PyGeoArrowResult<MultiPointArray> {
        Ok(MultiPointArray(self.0.clone().try_into()?))
    }

    fn to_multi_line_string_array(&self) -> PyGeoArrowResult<MultiLineStringArray> {
        Ok(MultiLineStringArray(self.0.clone().try_into()?))
    }

    fn to_multi_polygon_array(&self) -> PyGeoArrowResult<MultiPolygonArray> {
        Ok(MultiPolygonArray(self.0.clone().try_into()?))
    }
}

#[pymethods]
impl RectArray {
    /// Convert this array to a PolygonArray
    ///
    /// Returns:
    ///     Array with polygon geometries
    fn to_polygon_array(&self) -> PolygonArray {
        PolygonArray(self.0.clone().into())
    }
}

#[pymethods]
impl PointArray {
    /// Construct a PointArray from arrays of x and y values
    #[classmethod]
    fn from_xy(
        _cls: &Bound<PyType>,
        x: PyScalarBuffer<Float64Type>,
        y: PyScalarBuffer<Float64Type>,
    ) -> PyGeoArrowResult<Self> {
        let coords = SeparatedCoordBuffer::try_new([x.0, y.0])?;
        Ok(geoarrow::array::PointArray::new(coords.into(), None, Default::default()).into())
    }
}
