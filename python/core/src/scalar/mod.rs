use geoarrow::error::GeoArrowError;
use geoarrow::scalar::GeometryScalarArray;
use geoarrow::GeometryArrayTrait;
use geozero::ToJson;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyTuple};
use pyo3_arrow::ffi::to_array_pycapsules;

use crate::error::PyGeoArrowResult;

/// An immutable geometry scalar using GeoArrow's in-memory representation.
///
/// **Note**: for best performance, do as many operations as possible on arrays or chunked
/// arrays instead of scalars.
///
// This is modeled as a geospatial array of length 1
#[pyclass(module = "geoarrow.rust.core._rust", name = "Geometry", subclass)]
pub struct PyGeometry(pub(crate) GeometryScalarArray);

impl From<GeometryScalarArray> for PyGeometry {
    fn from(value: GeometryScalarArray) -> Self {
        Self(value)
    }
}

impl From<PyGeometry> for GeometryScalarArray {
    fn from(value: PyGeometry) -> Self {
        value.0
    }
}

impl PyGeometry {
    pub fn new(array: GeometryScalarArray) -> Self {
        Self(array)
    }

    pub fn inner(&self) -> &GeometryScalarArray {
        &self.0
    }

    pub fn into_inner(self) -> GeometryScalarArray {
        self.0
    }

    #[allow(clippy::should_implement_trait)]
    pub fn as_ref(&self) -> &dyn GeometryArrayTrait {
        self.0.inner().as_ref()
    }

    pub fn to_geo_point(&self) -> PyGeoArrowResult<geo::Point> {
        todo!()
    }

    pub fn to_geo_line_string(&self) -> PyGeoArrowResult<geo::LineString> {
        todo!()
    }

    pub fn to_geo(&self) -> geo::Geometry {
        todo!()
    }
}

#[pymethods]
impl PyGeometry {
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
        let geo_arr = self.0.inner();
        let field = geo_arr.extension_field();
        let array = geo_arr.to_array_ref();
        Ok(to_array_pycapsules(py, field, &array, requested_schema)?)
    }

    /// Check for equality with other object.
    pub fn __eq__(&self, _other: &PyGeometry) -> bool {
        todo!()
        // self.0 == other.0
    }

    /// Implements the "geo interface protocol".
    ///
    /// See <https://gist.github.com/sgillies/2217756>
    #[getter]
    pub fn __geo_interface__<'a>(&'a self, py: Python<'a>) -> PyGeoArrowResult<Bound<PyAny>> {
        let json_string = self.0.to_json().map_err(GeoArrowError::GeozeroError)?;
        let json_mod = py.import_bound(intern!(py, "json"))?;
        let args = (json_string.into_py(py),);

        Ok(json_mod.call_method1(intern!(py, "loads"), args)?)
    }

    /// Render as SVG
    pub fn _repr_svg_(&self) -> PyGeoArrowResult<String> {
        todo!()

        // let scalar = self.0;
        // let ([mut min_x, mut min_y], [mut max_x, mut max_y]) = bounding_rect_geometry(&scalar);

        // let mut svg_data = Vec::new();
        // // Passing `true` to `invert_y` is necessary to match Shapely's _repr_svg_
        // let mut svg = SvgWriter::new(&mut svg_data, true);

        // // Expand box by 10% for readability
        // min_x -= (max_x - min_x) * 0.05;
        // min_y -= (max_y - min_y) * 0.05;
        // max_x += (max_x - min_x) * 0.05;
        // max_y += (max_y - min_y) * 0.05;

        // svg.set_dimensions(min_x, min_y, max_x, max_y, 100, 100);

        // // This sequence is necessary so that the SvgWriter writes the header. See
        // // https://github.com/georust/geozero/blob/6c820ad7a0cac8c864058c783f548407427712d3/geozero/src/svg/mod.rs#L51-L58
        // svg.dataset_begin(None)
        //     .map_err(GeoArrowError::GeozeroError)?;
        // svg.feature_begin(0).map_err(GeoArrowError::GeozeroError)?;
        // scalar
        //     .process_geom(&mut svg)
        //     .map_err(GeoArrowError::GeozeroError)?;
        // svg.feature_end(0).map_err(GeoArrowError::GeozeroError)?;
        // svg.dataset_end().map_err(GeoArrowError::GeozeroError)?;

        // let string =
        //     String::from_utf8(svg_data).map_err(|err| PyIOError::new_err(err.to_string()))?;
        // Ok(string)
    }

    /// Text representation
    pub fn __repr__(&self) -> PyGeoArrowResult<String> {
        Ok("geoarrow.rust.core.Geometry".to_string())
        // todo!()
        // let scalar = <$geoarrow_scalar>::from(&self.0);
        // Ok(scalar.to_string())
    }
}
