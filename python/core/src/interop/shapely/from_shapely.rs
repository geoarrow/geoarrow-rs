use crate::array::*;
use crate::chunked_array::*;
use crate::error::{PyGeoArrowError, PyGeoArrowResult};
use crate::interop::shapely::utils::import_shapely;
use arrow_array::builder::{BinaryBuilder, Int32BufferBuilder};
use arrow_buffer::OffsetBuffer;
use geoarrow::array::CoordType;
use geoarrow::io::wkb::FromWKB;
use numpy::{PyReadonlyArray1, PyReadonlyArray2};
use pyo3::exceptions::PyValueError;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PySlice, PyString, PyType};
use pyo3::PyAny;

/// Check that the value of the GeometryType enum returned from shapely.to_ragged_array matches the
/// expected variant for this geometry array.
fn check_geometry_type(
    py: Python,
    shapely_mod: &PyModule,
    geom_type: &PyAny,
    expected_geom_type: &PyString,
) -> PyGeoArrowResult<()> {
    let shapely_enum = shapely_mod.getattr(intern!(py, "GeometryType"))?;
    if !geom_type.eq(shapely_enum.getattr(expected_geom_type)?)? {
        Err(PyValueError::new_err(format!(
            "Unexpected geometry type {}",
            geom_type.getattr(intern!(py, "name"))?,
        ))
        .into())
    } else {
        Ok(())
    }
}

/// Call shapely.to_ragged_array and validate expected geometry type.
fn call_to_ragged_array(
    py: Python,
    shapely_mod: &PyModule,
    input: &PyAny,
    expected_geom_type: &PyString,
) -> PyGeoArrowResult<(PyObject, PyObject)> {
    let args = (input,);

    let kwargs = PyDict::new(py);
    kwargs.set_item("include_z", false)?;
    let ragged_array_output =
        shapely_mod.call_method(intern!(py, "to_ragged_array"), args, Some(kwargs))?;

    let (geom_type, coords, offsets) =
        ragged_array_output.extract::<(PyObject, PyObject, PyObject)>()?;
    check_geometry_type(py, shapely_mod, geom_type.as_ref(py), expected_geom_type)?;

    Ok((coords, offsets))
}

/// Call shapely.to_wkb
fn call_to_wkb<'a>(
    py: Python<'a>,
    shapely_mod: &'a PyModule,
    input: &PyAny,
) -> PyGeoArrowResult<&'a PyAny> {
    let args = (input,);

    let kwargs = PyDict::new(py);
    kwargs.set_item("output_dimension", 2)?;
    kwargs.set_item("include_srid", false)?;
    kwargs.set_item("flavor", "iso")?;

    Ok(shapely_mod.call_method(intern!(py, "to_wkb"), args, Some(kwargs))?)
}

fn numpy_to_offsets_buffer(
    numpy_offsets: &PyReadonlyArray1<'_, i64>,
) -> PyGeoArrowResult<OffsetBuffer<i32>> {
    let offsets_slice = numpy_offsets
        .as_slice()
        .map_err(|err| PyGeoArrowError::PyErr(err.into()))?;
    let mut scalar_buffer = Int32BufferBuilder::new(offsets_slice.len());
    offsets_slice
        .iter()
        .for_each(|x| scalar_buffer.append((*x).try_into().unwrap()));
    Ok(OffsetBuffer::new(scalar_buffer.finish().into()))
}

/// Create a GeoArrow array from an array of Shapely geometries.
///
/// ### Notes:
///
/// - Currently this will always generate a non-chunked GeoArrow array. Use the `from_shapely`
/// method on a chunked GeoArrow array class to construct a chunked array.
/// - This will first call [`to_ragged_array`][shapely.to_ragged_array], falling back to
///   [`to_wkb`][shapely.to_wkb] if necessary. If you know you have mixed-type geometries in your
///   column, use [`MixedGeometryArray.from_shapely`][MixedGeometryArray.from_shapely]. '
///
///   This is because `to_ragged_array` is the fastest approach but fails on mixed-type geometries.
///   It supports combining Multi-* geometries with non-multi-geometries in the same array, so you
///   can combine e.g. Point and MultiPoint geometries in the same array, but `to_ragged_array`
///   doesn't work if you have Point and Polygon geometries in the same array.
///
/// Args:
///
///   input: Any array object accepted by Shapely, including numpy object arrays and
///   [`geopandas.GeoSeries`][geopandas.GeoSeries].
///
/// Returns:
///
///     A GeoArrow array
#[pyfunction]
pub fn from_shapely(py: Python, input: &PyAny) -> PyGeoArrowResult<PyObject> {
    let shapely_mod = import_shapely(py)?;

    let kwargs = PyDict::new(py);
    kwargs.set_item("include_z", false)?;
    if let Ok(ragged_array_output) =
        shapely_mod.call_method(intern!(py, "to_ragged_array"), (input,), Some(kwargs))
    {
        let (geom_type, coords_pyobj, offsets_pyobj) =
            ragged_array_output.extract::<(&PyAny, PyObject, PyObject)>()?;

        let geometry_type_enum = shapely_mod.getattr(intern!(py, "GeometryType"))?;

        if geom_type.eq(geometry_type_enum.getattr(intern!(py, "POINT"))?)? {
            Ok(PointArray::from_ragged_array(py, coords_pyobj, offsets_pyobj)?.into_py(py))
        } else if geom_type.eq(geometry_type_enum.getattr(intern!(py, "LINESTRING"))?)? {
            Ok(LineStringArray::from_ragged_array(py, coords_pyobj, offsets_pyobj)?.into_py(py))
        } else if geom_type.eq(geometry_type_enum.getattr(intern!(py, "POLYGON"))?)? {
            Ok(PolygonArray::from_ragged_array(py, coords_pyobj, offsets_pyobj)?.into_py(py))
        } else if geom_type.eq(geometry_type_enum.getattr(intern!(py, "MULTIPOINT"))?)? {
            Ok(MultiPointArray::from_ragged_array(py, coords_pyobj, offsets_pyobj)?.into_py(py))
        } else if geom_type.eq(geometry_type_enum.getattr(intern!(py, "MULTILINESTRING"))?)? {
            Ok(
                MultiLineStringArray::from_ragged_array(py, coords_pyobj, offsets_pyobj)?
                    .into_py(py),
            )
        } else if geom_type.eq(geometry_type_enum.getattr(intern!(py, "MULTIPOLYGON"))?)? {
            Ok(MultiPolygonArray::from_ragged_array(py, coords_pyobj, offsets_pyobj)?.into_py(py))
        } else {
            Err(PyValueError::new_err(format!(
                "unexpected geometry type from to_ragged_array {}",
                geom_type
            ))
            .into())
        }
    } else {
        Ok(MixedGeometryArray::from_shapely(py.get_type::<WKBArray>(), py, input)?.into_py(py))
    }
}

impl PointArray {
    fn from_ragged_array(
        py: Python,
        coords_pyobj: PyObject,
        _offsets_pyobj: PyObject,
    ) -> PyGeoArrowResult<Self> {
        let numpy_coords = coords_pyobj.extract::<PyReadonlyArray2<'_, f64>>(py)?;
        let coords_slice = numpy_coords
            .as_slice()
            .map_err(|err| PyGeoArrowError::PyErr(err.into()))?;
        let coords = geoarrow::array::InterleavedCoordBuffer::from(coords_slice).into();
        let point_array = geoarrow::array::PointArray::new(coords, None, Default::default());
        Ok(point_array.into())
    }
}

impl LineStringArray {
    fn from_ragged_array(
        py: Python,
        coords_pyobj: PyObject,
        offsets_pyobj: PyObject,
    ) -> PyGeoArrowResult<Self> {
        let numpy_coords = coords_pyobj.extract::<PyReadonlyArray2<'_, f64>>(py)?;
        let (numpy_geom_offsets,) = offsets_pyobj.extract::<(PyReadonlyArray1<'_, i64>,)>(py)?;

        let coords_slice = numpy_coords
            .as_slice()
            .map_err(|err| PyGeoArrowError::PyErr(err.into()))?;
        let geom_offsets = numpy_to_offsets_buffer(&numpy_geom_offsets)?;

        let coords = geoarrow::array::InterleavedCoordBuffer::from(coords_slice).into();
        let point_array =
            geoarrow::array::LineStringArray::new(coords, geom_offsets, None, Default::default());
        Ok(point_array.into())
    }
}

impl PolygonArray {
    fn from_ragged_array(
        py: Python,
        coords_pyobj: PyObject,
        offsets_pyobj: PyObject,
    ) -> PyGeoArrowResult<Self> {
        let numpy_coords = coords_pyobj.extract::<PyReadonlyArray2<'_, f64>>(py)?;
        let (numpy_ring_offsets, numpy_geom_offsets) =
            offsets_pyobj.extract::<(PyReadonlyArray1<'_, i64>, PyReadonlyArray1<'_, i64>)>(py)?;

        let coords_slice = numpy_coords
            .as_slice()
            .map_err(|err| PyGeoArrowError::PyErr(err.into()))?;
        let ring_offsets = numpy_to_offsets_buffer(&numpy_ring_offsets)?;
        let geom_offsets = numpy_to_offsets_buffer(&numpy_geom_offsets)?;

        let coords = geoarrow::array::InterleavedCoordBuffer::from(coords_slice).into();
        let point_array = geoarrow::array::PolygonArray::new(
            coords,
            geom_offsets,
            ring_offsets,
            None,
            Default::default(),
        );
        Ok(point_array.into())
    }
}

impl MultiPointArray {
    fn from_ragged_array(
        py: Python,
        coords_pyobj: PyObject,
        offsets_pyobj: PyObject,
    ) -> PyGeoArrowResult<Self> {
        let numpy_coords = coords_pyobj.extract::<PyReadonlyArray2<'_, f64>>(py)?;
        let (numpy_geom_offsets,) = offsets_pyobj.extract::<(PyReadonlyArray1<'_, i64>,)>(py)?;

        let coords_slice = numpy_coords
            .as_slice()
            .map_err(|err| PyGeoArrowError::PyErr(err.into()))?;
        let geom_offsets = numpy_to_offsets_buffer(&numpy_geom_offsets)?;

        let coords = geoarrow::array::InterleavedCoordBuffer::from(coords_slice).into();
        let point_array =
            geoarrow::array::MultiPointArray::new(coords, geom_offsets, None, Default::default());
        Ok(point_array.into())
    }
}

impl MultiLineStringArray {
    fn from_ragged_array(
        py: Python,
        coords_pyobj: PyObject,
        offsets_pyobj: PyObject,
    ) -> PyGeoArrowResult<Self> {
        let numpy_coords = coords_pyobj.extract::<PyReadonlyArray2<'_, f64>>(py)?;
        let (numpy_ring_offsets, numpy_geom_offsets) =
            offsets_pyobj.extract::<(PyReadonlyArray1<'_, i64>, PyReadonlyArray1<'_, i64>)>(py)?;

        let coords_slice = numpy_coords
            .as_slice()
            .map_err(|err| PyGeoArrowError::PyErr(err.into()))?;
        let ring_offsets = numpy_to_offsets_buffer(&numpy_ring_offsets)?;
        let geom_offsets = numpy_to_offsets_buffer(&numpy_geom_offsets)?;

        let coords = geoarrow::array::InterleavedCoordBuffer::from(coords_slice).into();
        let point_array = geoarrow::array::MultiLineStringArray::new(
            coords,
            geom_offsets,
            ring_offsets,
            None,
            Default::default(),
        );
        Ok(point_array.into())
    }
}

impl MultiPolygonArray {
    fn from_ragged_array(
        py: Python,
        coords_pyobj: PyObject,
        offsets_pyobj: PyObject,
    ) -> PyGeoArrowResult<Self> {
        let numpy_coords = coords_pyobj.extract::<PyReadonlyArray2<'_, f64>>(py)?;
        let (numpy_ring_offsets, numpy_polygon_offsets, numpy_geom_offsets) = offsets_pyobj
            .extract::<(
                PyReadonlyArray1<'_, i64>,
                PyReadonlyArray1<'_, i64>,
                PyReadonlyArray1<'_, i64>,
            )>(py)?;

        let coords_slice = numpy_coords
            .as_slice()
            .map_err(|err| PyGeoArrowError::PyErr(err.into()))?;
        let ring_offsets = numpy_to_offsets_buffer(&numpy_ring_offsets)?;
        let polygon_offsets = numpy_to_offsets_buffer(&numpy_polygon_offsets)?;
        let geom_offsets = numpy_to_offsets_buffer(&numpy_geom_offsets)?;

        let coords = geoarrow::array::InterleavedCoordBuffer::from(coords_slice).into();
        let point_array = geoarrow::array::MultiPolygonArray::new(
            coords,
            geom_offsets,
            polygon_offsets,
            ring_offsets,
            None,
            Default::default(),
        );
        Ok(point_array.into())
    }
}

macro_rules! impl_from_shapely_ragged_array {
    ($py_array_struct:ty, $expected_geom_type:literal) => {
        #[pymethods]
        impl $py_array_struct {
            /// Create this array from a shapely array
            ///
            /// Args:
            ///
            ///   input: Any array object accepted by [`shapely.to_ragged_array`][shapely.to_ragged_array], including numpy object arrays and
            ///   [`geopandas.GeoSeries`][geopandas.GeoSeries]
            ///
            /// Returns:
            ///
            ///     A new array.
            #[classmethod]
            fn from_shapely(_cls: &PyType, py: Python, input: &PyAny) -> PyGeoArrowResult<Self> {
                let shapely_mod = import_shapely(py)?;
                let (coords_pyobj, offsets_pyobj) =
                    call_to_ragged_array(py, shapely_mod, input, intern!(py, $expected_geom_type))?;
                Self::from_ragged_array(py, coords_pyobj, offsets_pyobj)
            }
        }
    };
}

impl_from_shapely_ragged_array!(PointArray, "POINT");
impl_from_shapely_ragged_array!(LineStringArray, "LINESTRING");
impl_from_shapely_ragged_array!(PolygonArray, "POLYGON");
impl_from_shapely_ragged_array!(MultiPointArray, "MULTIPOINT");
impl_from_shapely_ragged_array!(MultiLineStringArray, "MULTILINESTRING");
impl_from_shapely_ragged_array!(MultiPolygonArray, "MULTIPOLYGON");

#[pymethods]
impl MixedGeometryArray {
    /// Create this array from a shapely array
    ///
    /// Args:
    ///
    ///   input: Any array object accepted by [`shapely.to_wkb`][shapely.to_wkb], including numpy
    ///   object arrays and [`geopandas.GeoSeries`][geopandas.GeoSeries]
    ///
    /// Returns:
    ///
    ///     A new array.
    #[classmethod]
    fn from_shapely(_cls: &PyType, py: Python, input: &PyAny) -> PyGeoArrowResult<Self> {
        let wkb_array = WKBArray::from_shapely(_cls, py, input)?;
        Ok(
            geoarrow::array::MixedGeometryArray::from_wkb(&wkb_array.0, CoordType::Interleaved)?
                .into(),
        )
    }
}

#[pymethods]
impl GeometryCollectionArray {
    /// Create this array from a shapely array
    ///
    /// Args:
    ///
    ///   input: Any array object accepted by [`shapely.to_wkb`][shapely.to_wkb], including numpy
    ///   object arrays and [`geopandas.GeoSeries`][geopandas.GeoSeries]
    ///
    /// Returns:
    ///
    ///     A new array.
    #[classmethod]
    fn from_shapely(_cls: &PyType, py: Python, input: &PyAny) -> PyGeoArrowResult<Self> {
        let wkb_array = WKBArray::from_shapely(_cls, py, input)?;
        Ok(geoarrow::array::GeometryCollectionArray::from_wkb(
            &wkb_array.0,
            CoordType::Interleaved,
        )?
        .into())
    }
}

#[pymethods]
impl WKBArray {
    /// Create this array from a shapely array
    ///
    /// Args:
    ///
    ///   input: Any array object accepted by [`shapely.to_wkb`][shapely.to_wkb], including numpy
    ///   object arrays and [`geopandas.GeoSeries`][geopandas.GeoSeries]
    ///
    /// Returns:
    ///
    ///     A new array.
    #[classmethod]
    fn from_shapely(_cls: &PyType, py: Python, input: &PyAny) -> PyGeoArrowResult<Self> {
        let shapely_mod = import_shapely(py)?;
        let wkb_result = call_to_wkb(py, shapely_mod, input)?;

        let mut builder = BinaryBuilder::with_capacity(wkb_result.len()?, 0);

        for item in wkb_result.iter()? {
            let x = item?.extract::<&PyBytes>()?;
            builder.append_value(x.as_bytes());
        }

        Ok(geoarrow::array::WKBArray::new(builder.finish(), Default::default()).into())
    }
}

macro_rules! impl_chunked_from_shapely {
    ($py_chunked_struct:ty, $py_array_struct:ty) => {
        #[pymethods]
        impl $py_chunked_struct {
            /// Create this array from a shapely array
            ///
            /// Args:
            ///
            ///   input: Any array object accepted by
            ///   [`shapely.to_ragged_array`][shapely.to_ragged_array], including numpy object
            ///   arrays and [`geopandas.GeoSeries`][geopandas.GeoSeries]
            ///
            /// Other args:
            ///
            ///     chunk_size: Maximum number of items per chunk.
            ///
            /// Returns:
            ///
            ///     A new chunked array.
            #[classmethod]
            #[pyo3(signature = (input, *, chunk_size=65536))]
            fn from_shapely(
                _cls: &PyType,
                py: Python,
                input: &PyAny,
                chunk_size: usize,
            ) -> PyGeoArrowResult<Self> {
                let len = input.len()?;
                let num_chunks = (len as f64 / chunk_size as f64).ceil() as usize;
                let mut chunks = Vec::with_capacity(num_chunks);

                for chunk_idx in 0..num_chunks {
                    let slice = PySlice::new(
                        py,
                        (chunk_idx * chunk_size).try_into().unwrap(),
                        ((chunk_idx + 1) * chunk_size).try_into().unwrap(),
                        1,
                    );
                    let input_slice = input.get_item(slice)?;
                    chunks.push(<$py_array_struct>::from_shapely(_cls, py, input_slice)?.0);
                }

                Ok(geoarrow::chunked_array::ChunkedGeometryArray::new(chunks).into())
            }
        }
    };
}

impl_chunked_from_shapely!(ChunkedPointArray, PointArray);
impl_chunked_from_shapely!(ChunkedLineStringArray, LineStringArray);
impl_chunked_from_shapely!(ChunkedPolygonArray, PolygonArray);
impl_chunked_from_shapely!(ChunkedMultiPointArray, MultiPointArray);
impl_chunked_from_shapely!(ChunkedMultiLineStringArray, MultiLineStringArray);
impl_chunked_from_shapely!(ChunkedMultiPolygonArray, MultiPolygonArray);
impl_chunked_from_shapely!(ChunkedMixedGeometryArray, MixedGeometryArray);
impl_chunked_from_shapely!(ChunkedGeometryCollectionArray, GeometryCollectionArray);
impl_chunked_from_shapely!(ChunkedWKBArray, WKBArray);
