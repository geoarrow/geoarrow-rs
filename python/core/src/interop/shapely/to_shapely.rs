use crate::array::*;
use crate::chunked_array::*;
use crate::error::PyGeoArrowResult;
use crate::interop::shapely::utils::import_shapely;
use arrow_buffer::NullBuffer;
use geoarrow::array::{CoordBuffer, CoordType};
use geoarrow::trait_::GeometryArraySelfMethods;
use geoarrow::GeometryArrayTrait;
use numpy::ToPyArray;
use pyo3::exceptions::PyValueError;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::PyAny;

const NULL_VALUES_ERR_MSG: &str = "Cannot convert GeoArrow array with null values to Shapely";

fn check_nulls(nulls: Option<&NullBuffer>) -> PyGeoArrowResult<()> {
    if nulls.is_some_and(|x| x.null_count() > 0) {
        Err(PyValueError::new_err(NULL_VALUES_ERR_MSG).into())
    } else {
        Ok(())
    }
}

fn coords_to_numpy(py: Python, coords: CoordBuffer) -> PyGeoArrowResult<PyObject> {
    let interleaved_coords = match coords.into_coord_type(CoordType::Interleaved) {
        CoordBuffer::Interleaved(x) => x,
        _ => unreachable!(),
    };
    let arrow_arr = interleaved_coords.values_array();
    let (_data_type, scalar_buffer, _nulls) = arrow_arr.into_parts();

    let numpy_coords = scalar_buffer
        .to_pyarray(py)
        .reshape([scalar_buffer.len() / 2, 2])?;

    Ok(numpy_coords.to_object(py))
}

#[pymethods]
impl PointArray {
    /// Convert this array to a shapely array
    ///
    /// Returns:
    ///
    ///     A shapely array.
    fn to_shapely<'a>(&'a self, py: Python<'a>) -> PyGeoArrowResult<&'a PyAny> {
        check_nulls(self.0.nulls())?;

        let shapely_mod = import_shapely(py)?;
        let shapely_geom_type_enum = shapely_mod.getattr(intern!(py, "GeometryType"))?;

        let coords = coords_to_numpy(py, self.0.coords().clone())?;
        let args = (
            shapely_geom_type_enum.getattr(intern!(py, "POINT"))?,
            coords,
        );
        Ok(shapely_mod.call_method1(intern!(py, "from_ragged_array"), args)?)
    }
}

#[pymethods]
impl LineStringArray {
    /// Convert this array to a shapely array
    ///
    /// Returns:
    ///
    ///     A shapely array.
    fn to_shapely<'a>(&'a self, py: Python<'a>) -> PyGeoArrowResult<&'a PyAny> {
        check_nulls(self.0.nulls())?;

        let shapely_mod = import_shapely(py)?;
        let shapely_geom_type_enum = shapely_mod.getattr(intern!(py, "GeometryType"))?;

        let coords = coords_to_numpy(py, self.0.coords().clone())?;
        let offsets = (self.0.geom_offsets().to_pyarray(py),);

        let args = (
            shapely_geom_type_enum.getattr(intern!(py, "POINT"))?,
            coords,
            offsets,
        );
        Ok(shapely_mod.call_method1(intern!(py, "from_ragged_array"), args)?)
    }
}

#[pymethods]
impl PolygonArray {
    /// Convert this array to a shapely array
    ///
    /// Returns:
    ///
    ///     A shapely array.
    fn to_shapely<'a>(&'a self, py: Python<'a>) -> PyGeoArrowResult<&'a PyAny> {
        check_nulls(self.0.nulls())?;

        let shapely_mod = import_shapely(py)?;
        let shapely_geom_type_enum = shapely_mod.getattr(intern!(py, "GeometryType"))?;

        let coords = coords_to_numpy(py, self.0.coords().clone())?;
        let offsets = (
            self.0.ring_offsets().to_pyarray(py),
            self.0.geom_offsets().to_pyarray(py),
        );

        let args = (
            shapely_geom_type_enum.getattr(intern!(py, "POINT"))?,
            coords,
            offsets,
        );
        Ok(shapely_mod.call_method1(intern!(py, "from_ragged_array"), args)?)
    }
}

#[pymethods]
impl MultiPointArray {
    /// Convert this array to a shapely array
    ///
    /// Returns:
    ///
    ///     A shapely array.
    fn to_shapely<'a>(&'a self, py: Python<'a>) -> PyGeoArrowResult<&'a PyAny> {
        check_nulls(self.0.nulls())?;

        let shapely_mod = import_shapely(py)?;
        let shapely_geom_type_enum = shapely_mod.getattr(intern!(py, "GeometryType"))?;

        let coords = coords_to_numpy(py, self.0.coords().clone())?;
        let offsets = (self.0.geom_offsets().to_pyarray(py),);

        let args = (
            shapely_geom_type_enum.getattr(intern!(py, "POINT"))?,
            coords,
            offsets,
        );
        Ok(shapely_mod.call_method1(intern!(py, "from_ragged_array"), args)?)
    }
}

#[pymethods]
impl MultiLineStringArray {
    /// Convert this array to a shapely array
    ///
    /// Returns:
    ///
    ///     A shapely array.
    fn to_shapely<'a>(&'a self, py: Python<'a>) -> PyGeoArrowResult<&'a PyAny> {
        check_nulls(self.0.nulls())?;

        let shapely_mod = import_shapely(py)?;
        let shapely_geom_type_enum = shapely_mod.getattr(intern!(py, "GeometryType"))?;

        let coords = coords_to_numpy(py, self.0.coords().clone())?;
        let offsets = (
            self.0.ring_offsets().to_pyarray(py),
            self.0.geom_offsets().to_pyarray(py),
        );

        let args = (
            shapely_geom_type_enum.getattr(intern!(py, "POINT"))?,
            coords,
            offsets,
        );
        Ok(shapely_mod.call_method1(intern!(py, "from_ragged_array"), args)?)
    }
}

#[pymethods]
impl MultiPolygonArray {
    /// Convert this array to a shapely array
    ///
    /// Returns:
    ///
    ///     A shapely array.
    fn to_shapely<'a>(&'a self, py: Python<'a>) -> PyGeoArrowResult<&'a PyAny> {
        check_nulls(self.0.nulls())?;

        let shapely_mod = import_shapely(py)?;
        let shapely_geom_type_enum = shapely_mod.getattr(intern!(py, "GeometryType"))?;

        let coords = coords_to_numpy(py, self.0.coords().clone())?;
        let offsets = (
            self.0.ring_offsets().to_pyarray(py),
            self.0.polygon_offsets().to_pyarray(py),
            self.0.geom_offsets().to_pyarray(py),
        );

        let args = (
            shapely_geom_type_enum.getattr(intern!(py, "POINT"))?,
            coords,
            offsets,
        );
        Ok(shapely_mod.call_method1(intern!(py, "from_ragged_array"), args)?)
    }
}

macro_rules! impl_chunked_to_shapely {
    ($py_chunked_struct:ty, $py_array_struct:ident) => {
        #[pymethods]
        impl $py_chunked_struct {
            /// Convert this array to a shapely array
            ///
            /// Returns:
            ///
            ///     A shapely array.
            fn to_shapely<'a>(&'a self, py: Python<'a>) -> PyGeoArrowResult<&'a PyAny> {
                let numpy_mod = py.import(intern!(py, "numpy"))?;
                let shapely_chunks = self
                    .0
                    .chunks()
                    .iter()
                    .map(|chunk| {
                        Ok($py_array_struct(chunk.clone())
                            .to_shapely(py)?
                            .to_object(py))
                    })
                    .collect::<PyGeoArrowResult<Vec<_>>>()?;
                Ok(numpy_mod.call_method1(intern!(py, "concatenate"), (shapely_chunks,))?)
            }
        }
    };
}

impl_chunked_to_shapely!(ChunkedPointArray, PointArray);
impl_chunked_to_shapely!(ChunkedLineStringArray, LineStringArray);
impl_chunked_to_shapely!(ChunkedPolygonArray, PolygonArray);
impl_chunked_to_shapely!(ChunkedMultiPointArray, MultiPointArray);
impl_chunked_to_shapely!(ChunkedMultiLineStringArray, MultiLineStringArray);
impl_chunked_to_shapely!(ChunkedMultiPolygonArray, MultiPolygonArray);
// impl_chunked_to_shapely!(ChunkedMixedGeometryArray, MixedGeometryArray);
// impl_chunked_to_shapely!(ChunkedGeometryCollectionArray, GeometryCollectionArray);
// impl_chunked_to_shapely!(ChunkedWKBArray, WKBArray);
