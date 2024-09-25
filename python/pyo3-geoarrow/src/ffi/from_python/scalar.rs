use std::sync::Arc;

use crate::array::*;
use crate::scalar::*;
use geoarrow::array::MixedGeometryArray;
use geoarrow::io::geozero::ToMixedArray;
use geoarrow::scalar::GeometryScalar;
use geozero::geojson::GeoJsonString;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};
use pyo3::{intern, PyAny, PyResult};

impl<'a> FromPyObject<'a> for PyGeometry {
    fn extract_bound(ob: &Bound<'a, PyAny>) -> PyResult<Self> {
        let py = ob.py();
        // TODO: direct shapely conversion not via __geo_interface__
        if let Ok(geo_arr) = ob.extract::<PyNativeArray>() {
            let scalar = GeometryScalar::try_new(geo_arr.0.into_inner()).unwrap();
            Ok(PyGeometry::new(scalar))
        } else if ob.hasattr(intern!(py, "__geo_interface__"))? {
            let json_string = call_geo_interface(py, ob)?;

            // Parse GeoJSON to geometry scalar
            let reader = GeoJsonString(json_string);

            // TODO: we need a dynamic dimensionality reader
            let arr: MixedGeometryArray<i32, 2> = reader
                .to_mixed_geometry_array()
                .map_err(|err| PyValueError::new_err(err.to_string()))?;
            Ok(Self(
                GeometryScalar::try_new(Arc::new(arr))
                    .map_err(|err| PyValueError::new_err(err.to_string()))?,
            ))
        } else {
            Err(PyValueError::new_err(
                "Expected input to have __arrow_c_array__ or __geo_interface__ dunder methods",
            ))
        }
    }
}

/// Access Python `__geo_interface__` attribute and encode to JSON string
fn call_geo_interface(py: Python, ob: &Bound<PyAny>) -> PyResult<String> {
    let py_obj = ob.getattr("__geo_interface__")?;

    // Import JSON module
    let json_mod = py.import_bound(intern!(py, "json"))?;

    // Prepare json.dumps call
    let args = (py_obj,);
    let separators = PyTuple::new_bound(py, vec![',', ':']);
    let kwargs = PyDict::new_bound(py);
    kwargs.set_item("separators", separators)?;

    // Call json.dumps
    let json_dumped = json_mod.call_method(intern!(py, "dumps"), args, Some(&kwargs))?;
    json_dumped.extract()
}
