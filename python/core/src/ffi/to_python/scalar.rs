use crate::array::*;
use crate::error::PyGeoArrowResult;
use crate::scalar::*;

use pyo3::prelude::*;

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
            pub fn __arrow_c_array__(
                &self,
                requested_schema: Option<PyObject>,
            ) -> PyGeoArrowResult<PyObject> {
                $py_array(self.0.clone().into()).__arrow_c_array__(requested_schema)
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
