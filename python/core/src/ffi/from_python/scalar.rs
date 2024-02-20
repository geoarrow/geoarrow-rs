use crate::array::*;
use crate::scalar::*;
use geoarrow::io::geozero::ToGeometry;
use geoarrow::scalar::OwnedGeometry;
use geoarrow::trait_::GeometryArrayAccessor;
use geoarrow::GeometryArrayTrait;
use geozero::geojson::GeoJsonString;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};
use pyo3::{intern, PyAny, PyResult};

/// Access Python `__geo_interface__` attribute and encode to JSON string
fn call_geo_interface(py: Python, ob: &PyAny) -> PyResult<String> {
    let py_obj = ob.getattr("__geo_interface__")?;

    // Import JSON module
    let json_mod = py.import(intern!(py, "json"))?;

    // Prepare json.dumps call
    let args = (py_obj,);
    let separators = PyTuple::new(py, vec![',', ':']);
    let kwargs = PyDict::new(py);
    kwargs.set_item("separators", separators)?;

    // Call json.dumps
    let json_dumped = json_mod.call_method(intern!(py, "dumps"), args, Some(kwargs))?;
    json_dumped.extract()
}

macro_rules! impl_extract {
    ($py_scalar:ty, $py_array:ty, $rs_scalar_variant:path) => {
        impl<'a> FromPyObject<'a> for $py_scalar {
            fn extract(ob: &'a PyAny) -> PyResult<Self> {
                if ob.hasattr("__arrow_c_array__")? {
                    let arr = ob.extract::<$py_array>()?;
                    if arr.0.len() != 1 {
                        return Err(PyValueError::new_err(
                            "Expected scalar input; found != 1 elements in input array.",
                        ));
                    }
                    let scalar = arr.0.value(0);
                    Ok(Self(scalar.into()))
                } else if ob.hasattr("__geo_interface__")? {
                    let json_string = Python::with_gil(|py| call_geo_interface(py, ob))?;

                    // Parse GeoJSON to geometry scalar
                    let reader = GeoJsonString(json_string);
                    let geom = ToGeometry::<i32>::to_geometry(&reader).map_err(|err| {
                        PyValueError::new_err(format!("Unable to parse GeoJSON String: {}", err))
                    })?;
                    let geom = match geom {
                        $rs_scalar_variant(g) => g,
                        _ => return Err(PyValueError::new_err("Unexpected geometry type.")),
                    };
                    Ok(Self(geom))
                } else {
                    Err(PyValueError::new_err(
                        "Expected GeoArrow scalar or object implementing Geo Interface.",
                    ))
                }
            }
        }
    };
}

impl_extract!(Point, PointArray, OwnedGeometry::Point);
impl_extract!(LineString, LineStringArray, OwnedGeometry::LineString);
impl_extract!(Polygon, PolygonArray, OwnedGeometry::Polygon);
impl_extract!(MultiPoint, MultiPointArray, OwnedGeometry::MultiPoint);
impl_extract!(
    MultiLineString,
    MultiLineStringArray,
    OwnedGeometry::MultiLineString
);
impl_extract!(MultiPolygon, MultiPolygonArray, OwnedGeometry::MultiPolygon);
impl_extract!(
    GeometryCollection,
    GeometryCollectionArray,
    OwnedGeometry::GeometryCollection
);

impl<'a> FromPyObject<'a> for Geometry {
    fn extract(ob: &'a PyAny) -> PyResult<Self> {
        if ob.hasattr("__arrow_c_array__")? {
            let arr = ob.extract::<MixedGeometryArray>()?;
            if arr.0.len() != 1 {
                return Err(PyValueError::new_err(
                    "Expected scalar input; found != 1 elements in input array.",
                ));
            }
            let scalar = arr.0.value(0);
            Ok(Self(scalar.into()))
        } else if ob.hasattr("__geo_interface__")? {
            let json_string = Python::with_gil(|py| call_geo_interface(py, ob))?;

            // Parse GeoJSON to geometry scalar
            let reader = GeoJsonString(json_string);
            let geom = ToGeometry::<i32>::to_geometry(&reader).map_err(|err| {
                PyValueError::new_err(format!("Unable to parse GeoJSON String: {}", err))
            })?;
            Ok(Self(geom))
        } else {
            Err(PyValueError::new_err(
                "Expected GeoArrow scalar or object implementing Geo Interface.",
            ))
        }
    }
}
