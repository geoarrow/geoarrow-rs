use crate::error::PyGeoArrowResult;
use crate::ffi::from_python::AnyGeometryInput;
use crate::ffi::to_python::{chunked_geometry_array_to_pyobject, geometry_array_to_pyobject};
use crate::scalar::Point;
use geoarrow::algorithm::geo::Rotate;
use geoarrow::chunked_array::from_geoarrow_chunks;
use geoarrow::error::GeoArrowError;
use geoarrow::geo_traits::PointTrait;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

pub enum Origin {
    Center,
    Centroid,
    Point(geo::Point),
}

impl<'a> FromPyObject<'a> for Origin {
    fn extract_bound(ob: &Bound<'a, PyAny>) -> PyResult<Self> {
        if let Ok(s) = ob.extract::<String>() {
            match s.to_lowercase().as_str() {
                "center" => Ok(Self::Center),
                "centroid" => Ok(Self::Centroid),
                _ => Err(PyValueError::new_err("Unexpected origin method")),
            }
        } else if let Ok(point) = ob.extract::<Point>() {
            Ok(Self::Point(geo::Point::new(point.0.x(), point.0.y())))
        } else if let Ok(point) = ob.extract::<[f64; 2]>() {
            Ok(Self::Point(geo::Point::new(point[0], point[1])))
        } else {
            Err(PyValueError::new_err(
                "expected 'center', 'centroid', or (float, float) tuple",
            ))
        }
    }
}

/// Returns a rotated geometry on a 2D plane.
///
/// The angle of rotation is specified in degrees. Positive angles are counter-clockwise and
/// negative are clockwise rotations.
///
/// The point of origin can be a keyword 'center' for the bounding box center (default), 'centroid'
/// for the geometry's centroid, a Point object or a coordinate tuple (x0, y0).
#[pyfunction]
#[pyo3(
    signature = (geom, angle, *, origin = Origin::Center),
    text_signature = "(input, angle, *, origin = 'center')")
]
pub fn rotate(
    py: Python,
    geom: AnyGeometryInput,
    angle: f64,
    origin: Origin,
) -> PyGeoArrowResult<PyObject> {
    match geom {
        AnyGeometryInput::Array(arr) => {
            let out = match origin {
                Origin::Center => arr.as_ref().rotate_around_center(&angle)?,
                Origin::Centroid => arr.as_ref().rotate_around_centroid(&angle)?,
                Origin::Point(point) => arr.as_ref().rotate_around_point(&angle, point)?,
            };
            geometry_array_to_pyobject(py, out)
        }
        AnyGeometryInput::Chunked(chunked) => {
            let chunks = chunked.as_ref().geometry_chunks();
            let out = match origin {
                Origin::Center => chunks
                    .iter()
                    .map(|chunk| chunk.as_ref().rotate_around_center(&angle))
                    .collect::<Result<Vec<_>, GeoArrowError>>()?,
                Origin::Centroid => chunks
                    .iter()
                    .map(|chunk| chunk.as_ref().rotate_around_centroid(&angle))
                    .collect::<Result<Vec<_>, GeoArrowError>>()?,
                Origin::Point(point) => chunks
                    .iter()
                    .map(|chunk| chunk.as_ref().rotate_around_point(&angle, point))
                    .collect::<Result<Vec<_>, GeoArrowError>>()?,
            };
            let out_refs = out.iter().map(|x| x.as_ref()).collect::<Vec<_>>();
            chunked_geometry_array_to_pyobject(py, from_geoarrow_chunks(out_refs.as_slice())?)
        }
    }
}
