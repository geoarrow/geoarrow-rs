use std::sync::Arc;

use arrow_schema::ArrowError;
use geoarrow_array::array::from_arrow_array;
use geoarrow_array::{GeoArrowArray, GeoArrowType};
use geoarrow_cast::downcast::NativeType;
use geoarrow_schema::{
    BoxType, GeometryCollectionType, LineStringType, MultiLineStringType, MultiPointType,
    MultiPolygonType, PointType, PolygonType,
};
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyTuple, PyType};
use pyo3_arrow::ffi::{ArrayIterator, to_stream_pycapsule};
use pyo3_arrow::input::AnyArray;
use pyo3_arrow::{PyArrayReader, PyChunkedArray};

use crate::error::{PyGeoArrowError, PyGeoArrowResult};
use crate::{PyCoordType, PyGeoArrowArray, PyGeoArrowType};

#[pyclass(
    module = "geoarrow.rust.core",
    name = "ChunkedGeoArrowArray",
    subclass,
    frozen
)]
pub struct PyChunkedGeoArrowArray {
    chunks: Vec<Arc<dyn GeoArrowArray>>,
    data_type: GeoArrowType,
}

impl PyChunkedGeoArrowArray {
    pub fn new(chunks: Vec<Arc<dyn GeoArrowArray>>, data_type: GeoArrowType) -> Self {
        // TODO: validate all chunks have the same data type
        Self { chunks, data_type }
    }

    /// Import from a raw Arrow C Stream capsule
    pub fn from_arrow_pycapsule(capsule: &Bound<PyCapsule>) -> PyGeoArrowResult<Self> {
        PyChunkedArray::from_arrow_pycapsule(capsule)?.try_into()
    }

    /// Export to a geoarrow.rust.core.GeometryArray.
    ///
    /// This requires that you depend on geoarrow-rust-core from your Python package.
    pub fn to_geoarrow<'py>(&'py self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let geoarrow_mod = py.import(intern!(py, "geoarrow.rust.core"))?;
        geoarrow_mod
            .getattr(intern!(py, "ChunkedGeoArrowArray"))?
            .call_method1(
                intern!(py, "from_arrow_pycapsule"),
                PyTuple::new(py, vec![self.__arrow_c_stream__(py, None)?])?,
            )
    }

    /// Create a new PyChunkedArray from a vec of [ArrayRef]s, inferring their data type
    /// automatically.
    pub fn from_arrays(chunks: Vec<Arc<dyn GeoArrowArray>>) -> PyGeoArrowResult<Self> {
        if chunks.is_empty() {
            return Err(ArrowError::SchemaError(
                "Cannot infer data type from empty Vec<Arc<dyn GeoArrowArray>>".to_string(),
            )
            .into());
        }

        if !chunks
            .windows(2)
            .all(|w| w[0].data_type() == w[1].data_type())
        {
            return Err(ArrowError::SchemaError("Mismatched data types".to_string()).into());
        }

        let data_type = chunks[0].data_type();
        Ok(Self::new(chunks, data_type))
    }

    pub fn into_inner(self) -> (Vec<Arc<dyn GeoArrowArray>>, GeoArrowType) {
        (self.chunks, self.data_type)
    }
}

#[pymethods]
impl PyChunkedGeoArrowArray {
    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_stream__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyCapsule>>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let field = Arc::new(self.data_type.to_field("", true));
        let arrow_chunks = self
            .chunks
            .iter()
            .map(|x| x.to_array_ref())
            .collect::<Vec<_>>();

        let array_reader = Box::new(ArrayIterator::new(arrow_chunks.into_iter().map(Ok), field));
        Ok(to_stream_pycapsule(py, array_reader, requested_schema)?)
    }

    // /// Check for equality with other object.
    // fn __eq__(&self, _other: &PyNativeArray) -> bool {
    //     self.0 == other.0
    // }

    // fn __getitem__(&self, i: isize) -> PyGeoArrowResult<Option<PyGeometry>> {
    //     // Handle negative indexes from the end
    //     let i = if i < 0 {
    //         let i = self.__len__() as isize + i;
    //         if i < 0 {
    //             return Err(PyIndexError::new_err("Index out of range").into());
    //         }
    //         i as usize
    //     } else {
    //         i as usize
    //     };
    //     if i >= self.0.len() {
    //         return Err(PyIndexError::new_err("Index out of range").into());
    //     }

    //     let sliced = self.0.slice(i, 1)?;
    //     let geom_chunks = sliced.geometry_chunks();
    //     assert_eq!(geom_chunks.len(), 1);
    //     Ok(Some(PyGeometry(
    //         GeometryScalar::try_new(geom_chunks[0].clone()).unwrap(),
    //     )))
    // }

    fn __len__(&self) -> usize {
        self.chunks.iter().fold(0, |acc, arr| acc + arr.len())
    }

    fn __repr__(&self) -> String {
        // self.0.to_string()
        "geoarrow.rust.core.ChunkedGeometryArray".to_string()
    }

    #[classmethod]
    pub fn from_arrow(_cls: &Bound<PyType>, data: Self) -> Self {
        data
    }

    #[classmethod]
    #[pyo3(name = "from_arrow_pycapsule")]
    fn from_arrow_pycapsule_py(
        _cls: &Bound<PyType>,
        capsule: &Bound<PyCapsule>,
    ) -> PyGeoArrowResult<Self> {
        Self::from_arrow_pycapsule(capsule)
    }

    #[getter]
    fn null_count(&self) -> usize {
        self.chunks
            .iter()
            .map(|chunk| chunk.logical_null_count())
            .sum()
    }

    #[getter]
    fn num_chunks(&self) -> usize {
        self.chunks.len()
    }

    fn chunk(&self, i: usize) -> PyGeoArrowArray {
        PyGeoArrowArray::new(self.chunks[i].clone())
    }

    fn chunks(&self) -> Vec<PyGeoArrowArray> {
        self.chunks
            .iter()
            .map(|chunk| PyGeoArrowArray::new(chunk.clone()))
            .collect()
    }

    #[pyo3(signature = (to_type, /))]
    fn cast(&self, to_type: PyGeoArrowType) -> PyGeoArrowResult<Self> {
        let casted = self
            .chunks
            .iter()
            .map(|chunk| geoarrow_cast::cast::cast(chunk.as_ref(), to_type.as_ref()))
            .collect::<Result<Vec<_>, _>>()?;

        Self::from_arrays(casted)
    }

    fn downcast(&self, coord_type: PyCoordType) -> PyGeoArrowResult<Self> {
        if let Some((native_type, dim)) =
            geoarrow_cast::downcast::infer_downcast_type(self.chunks.iter().map(|x| x.as_ref()))?
        {
            let metadata = self.data_type.metadata().clone();
            let to_type = match native_type {
                NativeType::Point => PointType::new(coord_type.into(), dim, metadata).into(),
                NativeType::LineString => {
                    LineStringType::new(coord_type.into(), dim, metadata).into()
                }
                NativeType::Polygon => PolygonType::new(coord_type.into(), dim, metadata).into(),
                NativeType::MultiPoint => {
                    MultiPointType::new(coord_type.into(), dim, metadata).into()
                }
                NativeType::MultiLineString => {
                    MultiLineStringType::new(coord_type.into(), dim, metadata).into()
                }
                NativeType::MultiPolygon => {
                    MultiPolygonType::new(coord_type.into(), dim, metadata).into()
                }
                NativeType::GeometryCollection => {
                    GeometryCollectionType::new(coord_type.into(), dim, metadata).into()
                }
                NativeType::Rect => BoxType::new(dim, metadata).into(),
            };
            self.cast(PyGeoArrowType::new(to_type))
        } else {
            Ok(Self::new(self.chunks.clone(), self.data_type.clone()))
        }
    }

    #[getter]
    fn r#type(&self) -> PyGeoArrowType {
        self.data_type.clone().into()
    }
}

impl<'a> FromPyObject<'a> for PyChunkedGeoArrowArray {
    fn extract_bound(ob: &Bound<'a, PyAny>) -> PyResult<Self> {
        let chunked_array = ob.extract::<AnyArray>()?.into_chunked_array()?;
        chunked_array.try_into().map_err(PyErr::from)
    }
}

impl TryFrom<PyChunkedArray> for PyChunkedGeoArrowArray {
    type Error = PyGeoArrowError;

    fn try_from(value: PyChunkedArray) -> Result<Self, Self::Error> {
        let (chunks, field) = value.into_inner();
        let geo_chunks = chunks
            .iter()
            .map(|array| from_arrow_array(&array, &field))
            .collect::<Result<Vec<_>, _>>()?;
        let geo_data_type = GeoArrowType::try_from(field.as_ref())?;
        Ok(Self {
            chunks: geo_chunks,
            data_type: geo_data_type,
        })
    }
}

impl TryFrom<PyArrayReader> for PyChunkedGeoArrowArray {
    type Error = PyGeoArrowError;

    fn try_from(value: PyArrayReader) -> Result<Self, Self::Error> {
        value.into_chunked_array()?.try_into()
    }
}
