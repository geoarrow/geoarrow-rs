use crate::array::*;
use crate::chunked_array::*;
use crate::error::PyGeoArrowResult;
use crate::ffi::from_python::import_arrow_c_array;
use crate::interop::shapely::utils::import_shapely;
use arrow_buffer::NullBuffer;
use geoarrow::array::{from_arrow_array, AsGeometryArray, CoordBuffer, CoordType};
use geoarrow::datatypes::GeoDataType;
use geoarrow::io::wkb::to_wkb;
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

/// Convert a GeoArrow array to a numpy array of Shapely objects
///
/// Args:
///     input: input geometry array
///
/// Returns:
///     numpy array with Shapely objects
#[pyfunction]
pub fn to_shapely(py: Python, input: &PyAny) -> PyGeoArrowResult<PyObject> {
    let (array, field) = import_arrow_c_array(input)?;
    let array = from_arrow_array(&array, &field)?;
    match array.data_type() {
        GeoDataType::Point(_) => Ok(PointArray(array.as_ref().as_point().clone())
            .to_shapely(py)?
            .to_object(py)),
        GeoDataType::LineString(_) => Ok(LineStringArray(array.as_ref().as_line_string().clone())
            .to_shapely(py)?
            .to_object(py)),
        GeoDataType::Polygon(_) => Ok(PolygonArray(array.as_ref().as_polygon().clone())
            .to_shapely(py)?
            .to_object(py)),
        GeoDataType::MultiPoint(_) => Ok(MultiPointArray(array.as_ref().as_multi_point().clone())
            .to_shapely(py)?
            .to_object(py)),
        GeoDataType::MultiLineString(_) => Ok(MultiLineStringArray(
            array.as_ref().as_multi_line_string().clone(),
        )
        .to_shapely(py)?
        .to_object(py)),
        GeoDataType::MultiPolygon(_) => {
            Ok(MultiPolygonArray(array.as_ref().as_multi_polygon().clone())
                .to_shapely(py)?
                .to_object(py))
        }
        GeoDataType::Mixed(_) => {
            MixedGeometryArray(array.as_ref().as_mixed().clone()).to_shapely(py)
        }
        GeoDataType::GeometryCollection(_) => {
            GeometryCollectionArray(array.as_ref().as_geometry_collection().clone()).to_shapely(py)
        }
        GeoDataType::WKB => WKBArray(array.as_ref().as_wkb().clone()).to_shapely(py),
        t => Err(PyValueError::new_err(format!("unexpected type {:?}", t)).into()),
    }
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
            shapely_geom_type_enum.getattr(intern!(py, "LINESTRING"))?,
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
            shapely_geom_type_enum.getattr(intern!(py, "POLYGON"))?,
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
            shapely_geom_type_enum.getattr(intern!(py, "MULTIPOINT"))?,
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
            shapely_geom_type_enum.getattr(intern!(py, "MULTILINESTRING"))?,
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
            shapely_geom_type_enum.getattr(intern!(py, "MULTIPOLYGON"))?,
            coords,
            offsets,
        );
        Ok(shapely_mod.call_method1(intern!(py, "from_ragged_array"), args)?)
    }
}

#[pymethods]
impl MixedGeometryArray {
    /// Convert this array to a shapely array
    ///
    /// Returns:
    ///
    ///     A shapely array.
    fn to_shapely<'a>(&'a self, py: Python<'a>) -> PyGeoArrowResult<PyObject> {
        check_nulls(self.0.nulls())?;
        WKBArray(to_wkb(self.0.as_ref())).to_shapely(py)
    }
}

#[pymethods]
impl GeometryCollectionArray {
    /// Convert this array to a shapely array
    ///
    /// Returns:
    ///
    ///     A shapely array.
    fn to_shapely<'a>(&'a self, py: Python<'a>) -> PyGeoArrowResult<PyObject> {
        check_nulls(self.0.nulls())?;
        WKBArray(to_wkb(self.0.as_ref())).to_shapely(py)
    }
}

#[pymethods]
impl WKBArray {
    /// Convert this array to a shapely array
    ///
    /// Returns:
    ///
    ///     A shapely array.
    fn to_shapely<'a>(&'a self, py: Python<'a>) -> PyGeoArrowResult<PyObject> {
        check_nulls(self.0.nulls())?;
        let shapely_mod = import_shapely(py)?;
        let shapely_arr =
            shapely_mod.call_method1(intern!(py, "from_wkb"), (self.__array__(py)?,))?;
        Ok(shapely_arr.to_object(py))
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
            pub fn to_shapely<'a>(&'a self, py: Python<'a>) -> PyGeoArrowResult<&'a PyAny> {
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
impl_chunked_to_shapely!(ChunkedMixedGeometryArray, MixedGeometryArray);
impl_chunked_to_shapely!(ChunkedGeometryCollectionArray, GeometryCollectionArray);
impl_chunked_to_shapely!(ChunkedWKBArray, WKBArray);
