use geoarrow::algorithm::native::bounding_rect::bounding_rect_geometry;
use geoarrow::error::GeoArrowError;
use geoarrow::scalar::GeometryScalar;
use geoarrow::NativeArray;
use geozero::svg::SvgWriter;
use geozero::{FeatureProcessor, GeozeroGeometry, ToJson};
use pyo3::exceptions::PyIOError;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyTuple};
use pyo3_arrow::ffi::to_array_pycapsules;

use crate::error::PyGeoArrowResult;

/// This is modeled as a geospatial array of length 1
#[pyclass(module = "geoarrow.rust.core", name = "Geometry", subclass, frozen)]
pub struct PyGeometry(pub(crate) GeometryScalar);

impl PyGeometry {
    pub fn new(array: GeometryScalar) -> Self {
        Self(array)
    }

    pub fn inner(&self) -> &GeometryScalar {
        &self.0
    }

    pub fn into_inner(self) -> GeometryScalar {
        self.0
    }

    #[allow(clippy::should_implement_trait)]
    pub fn as_ref(&self) -> &dyn NativeArray {
        self.0.inner().as_ref()
    }

    pub fn to_geo_point(&self) -> PyGeoArrowResult<geo::Point> {
        Ok(self.inner().to_geo_point()?)
    }

    pub fn to_geo_line_string(&self) -> PyGeoArrowResult<geo::LineString> {
        Ok(self.inner().to_geo_line_string()?)
    }

    pub fn to_geo(&self) -> geo::Geometry {
        self.inner().to_geo()
    }
}

#[pymethods]
impl PyGeometry {
    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_array__<'py>(
        &'py self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyCapsule>>,
    ) -> PyGeoArrowResult<Bound<'py, PyTuple>> {
        let geo_arr = self.0.inner();
        let field = geo_arr.extension_field();
        let array = geo_arr.to_array_ref();
        Ok(to_array_pycapsules(py, field, &array, requested_schema)?)
    }

    // /// Check for equality with other object.
    // fn __eq__(&self, _other: &PyGeometry) -> bool {
    //     // self.0 == other.0
    // }

    #[getter]
    fn __geo_interface__<'py>(&'py self, py: Python<'py>) -> PyGeoArrowResult<Bound<'py, PyAny>> {
        let json_string = self.0.to_json().map_err(GeoArrowError::GeozeroError)?;
        let json_mod = py.import(intern!(py, "json"))?;
        Ok(json_mod.call_method1(intern!(py, "loads"), (json_string,))?)
    }

    fn _repr_svg_(&self) -> PyGeoArrowResult<String> {
        let scalar = self.0.to_geo();
        let ([mut min_x, mut min_y], [mut max_x, mut max_y]) = bounding_rect_geometry(&scalar);

        let mut svg_data = Vec::new();
        // Passing `true` to `invert_y` is necessary to match Shapely's _repr_svg_
        let mut svg = SvgWriter::new(&mut svg_data, true);

        // Expand box by 10% for readability
        min_x -= (max_x - min_x) * 0.05;
        min_y -= (max_y - min_y) * 0.05;
        max_x += (max_x - min_x) * 0.05;
        max_y += (max_y - min_y) * 0.05;

        svg.set_dimensions(min_x, min_y, max_x, max_y, 100, 100);

        // This sequence is necessary so that the SvgWriter writes the header. See
        // https://github.com/georust/geozero/blob/6c820ad7a0cac8c864058c783f548407427712d3/geozero/src/svg/mod.rs#L51-L58
        svg.dataset_begin(None)
            .map_err(GeoArrowError::GeozeroError)?;
        svg.feature_begin(0).map_err(GeoArrowError::GeozeroError)?;
        scalar
            .process_geom(&mut svg)
            .map_err(GeoArrowError::GeozeroError)?;
        svg.feature_end(0).map_err(GeoArrowError::GeozeroError)?;
        svg.dataset_end().map_err(GeoArrowError::GeozeroError)?;

        let string =
            String::from_utf8(svg_data).map_err(|err| PyIOError::new_err(err.to_string()))?;
        Ok(string)
    }

    fn __repr__(&self) -> PyGeoArrowResult<String> {
        Ok("geoarrow.rust.core.Geometry".to_string())
        // todo!()
        // let scalar = <$geoarrow_scalar>::from(&self.0);
        // Ok(scalar.to_string())
    }
}

impl From<GeometryScalar> for PyGeometry {
    fn from(value: GeometryScalar) -> Self {
        Self(value)
    }
}

impl From<PyGeometry> for GeometryScalar {
    fn from(value: PyGeometry) -> Self {
        value.0
    }
}
