#[cfg(feature = "geozero")]
mod bounding_rect;

use std::io::Write;
use std::sync::Arc;

use geoarrow_array::GeoArrowArray;
use geoarrow_array::cast::AsGeoArrowArray;
use geoarrow_schema::GeoArrowType;
use geoarrow_schema::error::GeoArrowError;
use pyo3::exceptions::{PyIOError, PyValueError};
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyTuple};
use pyo3_arrow::ffi::to_array_pycapsules;

use crate::PyGeoArray;
use crate::data_type::PyGeoArrowType;
use crate::error::PyGeoArrowResult;

/// This is modeled as a geospatial array of length 1
#[pyclass(module = "geoarrow.rust.core", name = "GeoScalar", subclass, frozen)]
pub struct PyGeoScalar(Arc<dyn GeoArrowArray>);

impl PyGeoScalar {
    pub fn try_new(array: Arc<dyn GeoArrowArray>) -> PyGeoArrowResult<Self> {
        if array.len() != 1 {
            Err(
                PyValueError::new_err("Scalar geometry must be backed by an array of length 1.")
                    .into(),
            )
        } else {
            Ok(Self(array))
        }
    }

    pub fn inner(&self) -> &Arc<dyn GeoArrowArray> {
        &self.0
    }

    pub fn into_inner(self) -> Arc<dyn GeoArrowArray> {
        self.0
    }
}

#[pymethods]
impl PyGeoScalar {
    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_array__<'py>(
        &'py self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyCapsule>>,
    ) -> PyGeoArrowResult<Bound<'py, PyTuple>> {
        let field = Arc::new(self.0.data_type().to_field("", true));
        let array = self.0.to_array_ref();
        Ok(to_array_pycapsules(py, field, &array, requested_schema)?)
    }

    fn __eq__(&self, other: &Bound<PyAny>) -> bool {
        // Do extraction within body because `__eq__` should never raise an exception.
        if let Ok(other) = other.extract::<Self>() {
            self.0.data_type() == other.0.data_type()
                && self.0.to_array_ref() == other.0.to_array_ref()
        } else {
            false
        }
    }

    #[cfg(feature = "geozero")]
    #[getter]
    fn __geo_interface__<'py>(&'py self, py: Python<'py>) -> PyGeoArrowResult<Bound<'py, PyAny>> {
        let json_string = to_json(&self.0).map_err(|err| GeoArrowError::External(Box::new(err)))?;
        let json_mod = py.import(intern!(py, "json"))?;
        Ok(json_mod.call_method1(intern!(py, "loads"), (json_string,))?)
    }

    #[cfg(feature = "geozero")]
    fn _repr_svg_(&self) -> PyGeoArrowResult<String> {
        use geozero::FeatureProcessor;

        use crate::scalar::bounding_rect::bounding_rect;

        let bounds = bounding_rect(&self.0)?.unwrap_or_default();
        let mut min_x = bounds.minx();
        let mut min_y = bounds.miny();
        let mut max_x = bounds.maxx();
        let mut max_y = bounds.maxy();

        let mut svg_data = Vec::new();
        // Passing `true` to `invert_y` is necessary to match Shapely's _repr_svg_
        let mut svg = geozero::svg::SvgWriter::new(&mut svg_data, true);

        // Expand box by 10% for readability
        min_x -= (max_x - min_x) * 0.05;
        min_y -= (max_y - min_y) * 0.05;
        max_x += (max_x - min_x) * 0.05;
        max_y += (max_y - min_y) * 0.05;

        svg.set_dimensions(min_x, min_y, max_x, max_y, 300, 300);

        // This sequence is necessary so that the SvgWriter writes the header. See
        // https://github.com/georust/geozero/blob/6c820ad7a0cac8c864058c783f548407427712d3/geozero/src/svg/mod.rs#L51-L58
        svg.dataset_begin(None)
            .map_err(|err| GeoArrowError::External(Box::new(err)))?;
        svg.feature_begin(0)
            .map_err(|err| GeoArrowError::External(Box::new(err)))?;
        if self.0.is_valid(0) {
            process_svg_geom(&self.0, &mut svg)
                .map_err(|err| GeoArrowError::External(Box::new(err)))?;
        }
        svg.feature_end(0)
            .map_err(|err| GeoArrowError::External(Box::new(err)))?;
        svg.dataset_end()
            .map_err(|err| GeoArrowError::External(Box::new(err)))?;

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

    #[getter]
    fn is_null(&self) -> bool {
        self.0.is_null(0)
    }

    #[getter]
    fn r#type(&self) -> PyGeoArrowType {
        self.0.data_type().into()
    }
}

impl<'a> FromPyObject<'a> for PyGeoScalar {
    fn extract_bound(ob: &Bound<'a, PyAny>) -> PyResult<Self> {
        Ok(Self::try_new(ob.extract::<PyGeoArray>()?.into_inner())?)
    }
}

#[cfg(feature = "geozero")]
fn process_svg_geom<W: Write>(
    arr: &dyn GeoArrowArray,
    svg: &mut geozero::svg::SvgWriter<W>,
) -> geozero::error::Result<()> {
    use GeoArrowType::*;
    use geozero::GeozeroGeometry;
    match arr.data_type() {
        Point(_) => arr.as_point().process_geom(svg),
        LineString(_) => arr.as_line_string().process_geom(svg),
        Polygon(_) => arr.as_polygon().process_geom(svg),
        MultiPoint(_) => arr.as_multi_point().process_geom(svg),
        MultiLineString(_) => arr.as_multi_line_string().process_geom(svg),
        MultiPolygon(_) => arr.as_multi_polygon().process_geom(svg),
        GeometryCollection(_) => arr.as_geometry_collection().process_geom(svg),
        Geometry(_) => arr.as_geometry().process_geom(svg),
        Rect(_) => arr.as_rect().process_geom(svg),
        Wkb(_) => arr.as_wkb::<i32>().process_geom(svg),
        LargeWkb(_) => arr.as_wkb::<i64>().process_geom(svg),
        WkbView(_) => arr.as_wkb_view().process_geom(svg),
        Wkt(_) => arr.as_wkt::<i32>().process_geom(svg),
        LargeWkt(_) => arr.as_wkt::<i64>().process_geom(svg),
        WktView(_) => arr.as_wkt_view().process_geom(svg),
    }
}

#[cfg(feature = "geozero")]
fn to_json(arr: &dyn GeoArrowArray) -> geozero::error::Result<String> {
    use GeoArrowType::*;
    use geozero::ToJson;
    match arr.data_type() {
        Point(_) => arr.as_point().to_json(),
        LineString(_) => arr.as_line_string().to_json(),
        Polygon(_) => arr.as_polygon().to_json(),
        MultiPoint(_) => arr.as_multi_point().to_json(),
        MultiLineString(_) => arr.as_multi_line_string().to_json(),
        MultiPolygon(_) => arr.as_multi_polygon().to_json(),
        GeometryCollection(_) => arr.as_geometry_collection().to_json(),
        Geometry(_) => arr.as_geometry().to_json(),
        Rect(_) => arr.as_rect().to_json(),
        Wkb(_) => arr.as_wkb::<i32>().to_json(),
        LargeWkb(_) => arr.as_wkb::<i64>().to_json(),
        WkbView(_) => arr.as_wkb_view().to_json(),
        Wkt(_) => arr.as_wkt::<i32>().to_json(),
        LargeWkt(_) => arr.as_wkt::<i64>().to_json(),
        WktView(_) => arr.as_wkt_view().to_json(),
    }
}
