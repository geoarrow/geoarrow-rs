use crate::array::*;
use crate::scalar::*;
use geoarrow::scalar::GeometryScalarArray;
use pyo3::prelude::*;
use pyo3::{PyAny, PyResult};

impl<'a> FromPyObject<'a> for PyGeometry {
    fn extract_bound(ob: &Bound<'a, PyAny>) -> PyResult<Self> {
        let geo_arr = ob.extract::<PyGeometryArray>()?;
        let scalar = GeometryScalarArray::try_new(geo_arr.0.into_inner()).unwrap();
        Ok(PyGeometry::new(scalar))
    }
}

// /// Access Python `__geo_interface__` attribute and encode to JSON string
// fn call_geo_interface(py: Python, ob: &PyAny) -> PyResult<String> {
//     let py_obj = ob.getattr("__geo_interface__")?;

//     // Import JSON module
//     let json_mod = py.import_bound(intern!(py, "json"))?;

//     // Prepare json.dumps call
//     let args = (py_obj,);
//     let separators = PyTuple::new_bound(py, vec![',', ':']);
//     let kwargs = PyDict::new_bound(py);
//     kwargs.set_item("separators", separators)?;

//     // Call json.dumps
//     let json_dumped = json_mod.call_method(intern!(py, "dumps"), args, Some(&kwargs))?;
//     json_dumped.extract()
// }

// macro_rules! impl_extract {
//     ($py_scalar:ty, $py_array:ty, $rs_scalar_variant:path) => {
//         impl<'a> FromPyObject<'a> for $py_scalar {
//             fn extract(ob: &'a PyAny) -> PyResult<Self> {
//                 if ob.hasattr("__arrow_c_array__")? {
//                     let arr = ob.extract::<$py_array>()?;
//                     if arr.0.len() != 1 {
//                         return Err(PyValueError::new_err(
//                             "Expected scalar input; found != 1 elements in input array.",
//                         ));
//                     }
//                     let scalar = arr.0.value(0);
//                     Ok(Self(scalar.into()))
//                 } else if ob.hasattr("__geo_interface__")? {
//                     let json_string = Python::with_gil(|py| call_geo_interface(py, ob))?;

//                     // Parse GeoJSON to geometry scalar
//                     let reader = GeoJsonString(json_string);
//                     let geom = ToGeometry::<i32>::to_geometry(&reader).map_err(|err| {
//                         PyValueError::new_err(format!("Unable to parse GeoJSON String: {}", err))
//                     })?;
//                     let geom = match geom {
//                         $rs_scalar_variant(g) => g,
//                         _ => return Err(PyValueError::new_err("Unexpected geometry type.")),
//                     };
//                     Ok(Self(geom))
//                 } else {
//                     Err(PyValueError::new_err(
//                         "Expected GeoArrow scalar or object implementing Geo Interface.",
//                     ))
//                 }
//             }
//         }
//     };
// }

// impl_extract!(Point, PointArray, OwnedGeometry::Point);
// impl_extract!(LineString, LineStringArray, OwnedGeometry::LineString);
// impl_extract!(Polygon, PolygonArray, OwnedGeometry::Polygon);
// impl_extract!(MultiPoint, MultiPointArray, OwnedGeometry::MultiPoint);
// impl_extract!(
//     MultiLineString,
//     MultiLineStringArray,
//     OwnedGeometry::MultiLineString
// );
// impl_extract!(MultiPolygon, MultiPolygonArray, OwnedGeometry::MultiPolygon);
// impl_extract!(
//     GeometryCollection,
//     GeometryCollectionArray,
//     OwnedGeometry::GeometryCollection
// );

// impl<'a> FromPyObject<'a> for Geometry {
//     fn extract(ob: &'a PyAny) -> PyResult<Self> {
//         if ob.hasattr("__arrow_c_array__")? {
//             let input = ob.extract::<GeometryArrayInput>()?;
//             let arr_ref = input.0.as_ref();
//             if arr_ref.len() != 1 {
//                 return Err(PyValueError::new_err(
//                     "Expected scalar input; found != 1 elements in input array.",
//                 ));
//             }
//             if arr_ref.is_null(0) {
//                 return Err(PyValueError::new_err("Scalar value is null"));
//             }

//             use Dimension::*;
//             use GeoDataType::*;

//             let scalar = match arr_ref.data_type() {
//                 Point(_, XY) => geoarrow::scalar::Geometry::Point(arr_ref.as_point::<2>().value(0)),
//                 LineString(_, XY) => {
//                     geoarrow::scalar::Geometry::LineString(arr_ref.as_line_string::<2>().value(0))
//                 }
//                 Polygon(_, XY) => {
//                     geoarrow::scalar::Geometry::Polygon(arr_ref.as_polygon::<2>().value(0))
//                 }
//                 MultiPoint(_, XY) => {
//                     geoarrow::scalar::Geometry::MultiPoint(arr_ref.as_multi_point::<2>().value(0))
//                 }
//                 MultiLineString(_, XY) => geoarrow::scalar::Geometry::MultiLineString(
//                     arr_ref.as_multi_line_string::<2>().value(0),
//                 ),
//                 MultiPolygon(_, XY) => geoarrow::scalar::Geometry::MultiPolygon(
//                     arr_ref.as_multi_polygon::<2>().value(0),
//                 ),
//                 Mixed(_, XY) => arr_ref.as_mixed::<2>().value(0),
//                 GeometryCollection(_, XY) => geoarrow::scalar::Geometry::GeometryCollection(
//                     arr_ref.as_geometry_collection::<2>().value(0),
//                 ),
//                 Rect(XY) => geoarrow::scalar::Geometry::Rect(arr_ref.as_rect::<2>().value(0)),

//                 dt => {
//                     return Err(PyValueError::new_err(format!(
//                         "Unsupported scalar array type: {:?}",
//                         dt
//                     )))
//                 }
//             };

//             Ok(Self(scalar.into()))
//         } else if ob.hasattr("__geo_interface__")? {
//             let json_string = Python::with_gil(|py| call_geo_interface(py, ob))?;

//             // Parse GeoJSON to geometry scalar
//             let reader = GeoJsonString(json_string);
//             let geom = ToGeometry::<i32>::to_geometry(&reader).map_err(|err| {
//                 PyValueError::new_err(format!("Unable to parse GeoJSON String: {}", err))
//             })?;
//             Ok(Self(geom))
//         } else {
//             Err(PyValueError::new_err(
//                 "Expected GeoArrow scalar or object implementing Geo Interface.",
//             ))
//         }
//     }
// }
