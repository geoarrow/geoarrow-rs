use crate::error::PyGeoArrowResult;
use crate::scalar::PyGeometry;
use geoarrow::array::GeometryArrayDyn;

use geoarrow::scalar::GeometryScalarArray;
use geoarrow::trait_::GeometryArrayRef;
use geoarrow::GeometryArrayTrait;
use pyo3::exceptions::PyIndexError;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyTuple, PyType};
use pyo3_arrow::ffi::to_array_pycapsules;

/// An immutable array of geometries using GeoArrow's in-memory representation.
#[pyclass(module = "geoarrow.rust.core._rust", name = "GeometryArray", subclass)]
pub struct PyGeometryArray(pub(crate) GeometryArrayDyn);

impl From<GeometryArrayDyn> for PyGeometryArray {
    fn from(value: GeometryArrayDyn) -> Self {
        Self(value)
    }
}

impl From<GeometryArrayRef> for PyGeometryArray {
    fn from(value: GeometryArrayRef) -> Self {
        Self(GeometryArrayDyn::new(value))
    }
}

impl From<PyGeometryArray> for GeometryArrayDyn {
    fn from(value: PyGeometryArray) -> Self {
        value.0
    }
}

impl PyGeometryArray {
    pub fn new(array: GeometryArrayDyn) -> Self {
        Self(array)
    }

    #[allow(clippy::should_implement_trait)]
    pub fn as_ref(&self) -> &dyn GeometryArrayTrait {
        self.0.as_ref()
    }
}

#[pymethods]
impl PyGeometryArray {
    /// An implementation of the [Arrow PyCapsule
    /// Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html).
    /// This dunder method should not be called directly, but enables zero-copy
    /// data transfer to other Python libraries that understand Arrow memory.
    ///
    /// For example, you can call [`pyarrow.array()`][pyarrow.array] to convert this array
    /// into a pyarrow array, without copying memory.
    #[allow(unused_variables)]
    pub fn __arrow_c_array__<'py>(
        &'py self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyCapsule>>,
    ) -> PyGeoArrowResult<Bound<PyTuple>> {
        let field = self.0.extension_field();
        let array = self.0.to_array_ref();
        Ok(to_array_pycapsules(py, field, &array, requested_schema)?)
    }

    /// Check for equality with other object.
    pub fn __eq__(&self, _other: &PyGeometryArray) -> bool {
        todo!()
        // self.0 == other.0
    }

    /// Implements the "geo interface protocol".
    ///
    /// See <https://gist.github.com/sgillies/2217756>
    #[getter]
    pub fn __geo_interface__<'a>(&'a self, _py: Python<'a>) -> PyGeoArrowResult<Bound<PyAny>> {
        todo!()
        // // Note: We use the lower-level GeoJsonWriter API directly so that we can force
        // // each geometry to be its own Feature. This is the format that GeoPandas expects,
        // // e.g. in GeoDataFrame.from_features(our_array)
        // let mut json_data = Vec::new();
        // let mut geojson_writer = GeoJsonWriter::new(&mut json_data);

        // geojson_writer
        //     .dataset_begin(None)
        //     .map_err(GeoArrowError::GeozeroError)?;

        // // TODO: what to do with missing values?
        // for (idx, geom) in self.0.iter().flatten().enumerate() {
        //     geojson_writer
        //         .feature_begin(idx as u64)
        //         .map_err(GeoArrowError::GeozeroError)?;
        //     geojson_writer
        //         .properties_begin()
        //         .map_err(GeoArrowError::GeozeroError)?;
        //     geojson_writer
        //         .properties_end()
        //         .map_err(GeoArrowError::GeozeroError)?;
        //     geojson_writer
        //         .geometry_begin()
        //         .map_err(GeoArrowError::GeozeroError)?;
        //     geom.process_geom(&mut geojson_writer)
        //         .map_err(GeoArrowError::GeozeroError)?;
        //     geojson_writer
        //         .geometry_end()
        //         .map_err(GeoArrowError::GeozeroError)?;
        //     geojson_writer
        //         .feature_end(idx as u64)
        //         .map_err(GeoArrowError::GeozeroError)?;
        // }

        // geojson_writer
        //     .dataset_end()
        //     .map_err(GeoArrowError::GeozeroError)?;

        // let json_string =
        //     String::from_utf8(json_data).map_err(|err| PyIOError::new_err(err.to_string()))?;
        // let json_mod = py.import_bound(intern!(py, "json"))?;
        // let args = (json_string.into_py(py),);
        // Ok(json_mod.call_method1(intern!(py, "loads"), args)?)
    }

    /// Access the item at a given index
    pub fn __getitem__(&self, i: isize) -> PyGeoArrowResult<Option<PyGeometry>> {
        // Handle negative indexes from the end
        let i = if i < 0 {
            let i = self.0.len() as isize + i;
            if i < 0 {
                return Err(PyIndexError::new_err("Index out of range").into());
            }
            i as usize
        } else {
            i as usize
        };
        if i >= self.0.len() {
            return Err(PyIndexError::new_err("Index out of range").into());
        }

        Ok(Some(PyGeometry(
            GeometryScalarArray::try_new(self.0.slice(i, 1)).unwrap(),
        )))
    }

    /// The number of rows
    pub fn __len__(&self) -> usize {
        self.0.len()
    }

    /// Text representation
    pub fn __repr__(&self) -> String {
        self.0.to_string()
    }

    // TODO: move this to the constructor instead.

    /// Construct this object from existing Arrow data
    ///
    /// Args:
    ///     input: Arrow array to use for constructing this object
    ///
    /// Returns:
    ///     Self
    #[classmethod]
    pub fn from_arrow(_cls: &Bound<PyType>, input: &Bound<PyAny>) -> PyResult<Self> {
        input.extract()
    }
}
