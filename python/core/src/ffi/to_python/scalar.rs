use crate::array::*;
use crate::error::PyGeoArrowResult;
use crate::scalar::*;

use geoarrow::GeometryArrayTrait;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyTuple};
use pyo3_arrow::ffi::to_array_pycapsules;

macro_rules! impl_arrow_c_array {
    ($struct_name:ident, $py_array:ident) => {
        #[pymethods]
        impl $struct_name {
            /// An implementation of the [Arrow PyCapsule
            /// Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html).
            /// This dunder method should not be called directly, but enables zero-copy
            /// data transfer to other Python libraries that understand Arrow memory.
            ///
            /// For example, you can call [`pyarrow.array()`][pyarrow.array] to convert this array
            /// into a pyarrow array, without copying memory.
            pub fn __arrow_c_array__<'py>(
                &'py self,
                py: Python<'py>,
                requested_schema: Option<Bound<'py, PyCapsule>>,
            ) -> PyGeoArrowResult<Bound<PyTuple>> {
                let arr = $py_array(self.0.clone().into());
                let field = arr.0.extension_field();
                let array = arr.0.to_array_ref();
                Ok(to_array_pycapsules(py, field, &array, requested_schema)?)
            }
        }
    };
}

impl_arrow_c_array!(Point, PointArray);
impl_arrow_c_array!(LineString, LineStringArray);
impl_arrow_c_array!(Polygon, PolygonArray);
impl_arrow_c_array!(MultiPoint, MultiPointArray);
impl_arrow_c_array!(MultiLineString, MultiLineStringArray);
impl_arrow_c_array!(MultiPolygon, MultiPolygonArray);
// impl_arrow_c_array!(Geometry, MixedGeometryArray);
impl_arrow_c_array!(GeometryCollection, GeometryCollectionArray);
impl_arrow_c_array!(Rect, RectArray);
