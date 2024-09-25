use std::sync::Arc;

use crate::crs::CRS;
use crate::ffi::to_python::geometry_array_to_pyobject;
use crate::interop::shapely::utils::import_shapely;
use arrow_array::builder::{BinaryBuilder, Int32BufferBuilder};
use arrow_buffer::OffsetBuffer;
use geoarrow::array::metadata::ArrayMetadata;
use geoarrow::array::InterleavedCoordBuffer;
use geoarrow::datatypes::{Dimension, NativeType};
use geoarrow::NativeArray;
use numpy::{PyReadonlyArray1, PyReadonlyArray2, PyUntypedArrayMethods};
use pyo3::exceptions::PyValueError;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyString};
use pyo3::PyAny;
use pyo3_geoarrow::PyGeoArrowResult;

/// Check that the value of the GeometryType enum returned from shapely.to_ragged_array matches the
/// expected variant for this geometry array.
#[allow(dead_code)]
fn check_geometry_type(
    py: Python,
    shapely_mod: &Bound<PyModule>,
    geom_type: &Bound<PyAny>,
    expected_geom_type: &Bound<PyString>,
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
#[allow(dead_code)]
fn call_to_ragged_array(
    py: Python,
    shapely_mod: &Bound<PyModule>,
    input: &Bound<PyAny>,
    expected_geom_type: &Bound<PyString>,
) -> PyGeoArrowResult<(PyObject, PyObject)> {
    let args = (input,);

    let kwargs = PyDict::new_bound(py);
    kwargs.set_item("include_z", false)?;
    let ragged_array_output =
        shapely_mod.call_method(intern!(py, "to_ragged_array"), args, Some(&kwargs))?;

    let (geom_type, coords, offsets) =
        ragged_array_output.extract::<(PyObject, PyObject, PyObject)>()?;
    check_geometry_type(py, shapely_mod, geom_type.bind(py), expected_geom_type)?;

    Ok((coords, offsets))
}

/// Call shapely.to_wkb
fn call_to_wkb<'a>(
    py: Python<'a>,
    shapely_mod: &'a Bound<PyModule>,
    input: &'a Bound<PyAny>,
) -> PyGeoArrowResult<Bound<'a, PyAny>> {
    let args = (input,);

    let kwargs = PyDict::new_bound(py);
    kwargs.set_item("output_dimension", 2)?;
    kwargs.set_item("include_srid", false)?;
    kwargs.set_item("flavor", "iso")?;

    Ok(shapely_mod.call_method(intern!(py, "to_wkb"), args, Some(&kwargs))?)
}

#[pyfunction]
#[pyo3(signature = (input, *, crs = None))]
pub fn from_shapely(
    py: Python,
    input: &Bound<PyAny>,
    crs: Option<CRS>,
) -> PyGeoArrowResult<PyObject> {
    let shapely_mod = import_shapely(py)?;

    let metadata = Arc::new(ArrayMetadata {
        crs: crs.map(|c| c.into_inner()),
        ..Default::default()
    });

    let kwargs = PyDict::new_bound(py);
    if let Ok(ragged_array_output) =
        shapely_mod.call_method(intern!(py, "to_ragged_array"), (input,), Some(&kwargs))
    {
        let (geom_type, coords, offsets) =
            ragged_array_output.extract::<(&PyAny, PyReadonlyArray2<'_, f64>, PyObject)>()?;
        let dim = Dimension::try_from(coords.shape()[1])?;

        let geometry_type_enum = shapely_mod.getattr(intern!(py, "GeometryType"))?;

        let arr = if geom_type.eq(geometry_type_enum.getattr(intern!(py, "POINT"))?)? {
            make_point_arr(coords, offsets, dim, metadata)?
        } else if geom_type.eq(geometry_type_enum.getattr(intern!(py, "LINESTRING"))?)? {
            make_linestring_arr(py, coords, offsets, dim, metadata)?
        } else if geom_type.eq(geometry_type_enum.getattr(intern!(py, "POLYGON"))?)? {
            make_polygon_arr(py, coords, offsets, dim, metadata)?
        } else if geom_type.eq(geometry_type_enum.getattr(intern!(py, "MULTIPOINT"))?)? {
            make_multipoint_arr(py, coords, offsets, dim, metadata)?
        } else if geom_type.eq(geometry_type_enum.getattr(intern!(py, "MULTILINESTRING"))?)? {
            make_multilinestring_arr(py, coords, offsets, dim, metadata)?
        } else if geom_type.eq(geometry_type_enum.getattr(intern!(py, "MULTIPOLYGON"))?)? {
            make_multipolygon_arr(py, coords, offsets, dim, metadata)?
        } else {
            return Err(PyValueError::new_err(format!(
                "unexpected geometry type from to_ragged_array {}",
                geom_type
            ))
            .into());
        };

        geometry_array_to_pyobject(py, arr)
    } else {
        // TODO: support 3d WKB
        let wkb_arr = make_wkb_arr(py, input, metadata)?;
        let geom_arr = geoarrow::io::wkb::from_wkb(
            &wkb_arr,
            NativeType::GeometryCollection(Default::default(), Dimension::XY),
            false,
        )?;
        geometry_array_to_pyobject(py, geom_arr)
    }
}

fn coords_to_buffer<const D: usize>(
    coords: PyReadonlyArray2<'_, f64>,
) -> PyGeoArrowResult<InterleavedCoordBuffer<D>> {
    let shape = coords.shape();
    assert_eq!(shape.len(), 2);
    let capacity = shape[0] * shape[1];

    let mut builder = Vec::with_capacity(capacity);
    for c in coords.as_array().into_iter() {
        builder.push(*c)
    }
    Ok(builder.try_into()?)
}

fn numpy_to_offsets(offsets: &PyReadonlyArray1<'_, i64>) -> PyGeoArrowResult<OffsetBuffer<i32>> {
    let mut builder = Int32BufferBuilder::new(offsets.len());
    for o in offsets.as_array().into_iter() {
        builder.append((*o).try_into().unwrap());
    }
    Ok(OffsetBuffer::new(builder.finish().into()))
}

fn make_point_arr(
    coords: PyReadonlyArray2<'_, f64>,
    _offsets: PyObject,
    dim: Dimension,
    metadata: Arc<ArrayMetadata>,
) -> PyGeoArrowResult<Arc<dyn NativeArray>> {
    match dim {
        Dimension::XY => {
            let cb = coords_to_buffer(coords)?;
            Ok(Arc::new(geoarrow::array::PointArray::<2>::new(
                cb.into(),
                None,
                metadata,
            )))
        }
        Dimension::XYZ => {
            let cb = coords_to_buffer(coords)?;
            Ok(Arc::new(geoarrow::array::PointArray::<3>::new(
                cb.into(),
                None,
                metadata,
            )))
        }
    }
}

fn make_linestring_arr(
    py: Python,
    coords: PyReadonlyArray2<'_, f64>,
    offsets: PyObject,
    dim: Dimension,
    metadata: Arc<ArrayMetadata>,
) -> PyGeoArrowResult<Arc<dyn NativeArray>> {
    let (geom_offsets,) = offsets.extract::<(PyReadonlyArray1<'_, i64>,)>(py)?;
    let geom_offsets = numpy_to_offsets(&geom_offsets)?;
    match dim {
        Dimension::XY => {
            let cb = coords_to_buffer(coords)?;
            Ok(Arc::new(geoarrow::array::LineStringArray::<i32, 2>::new(
                cb.into(),
                geom_offsets,
                None,
                metadata,
            )))
        }
        Dimension::XYZ => {
            let cb = coords_to_buffer(coords)?;
            Ok(Arc::new(geoarrow::array::LineStringArray::<i32, 3>::new(
                cb.into(),
                geom_offsets,
                None,
                metadata,
            )))
        }
    }
}

fn make_polygon_arr(
    py: Python,
    coords: PyReadonlyArray2<'_, f64>,
    offsets: PyObject,
    dim: Dimension,
    metadata: Arc<ArrayMetadata>,
) -> PyGeoArrowResult<Arc<dyn NativeArray>> {
    let (ring_offsets, geom_offsets) =
        offsets.extract::<(PyReadonlyArray1<'_, i64>, PyReadonlyArray1<'_, i64>)>(py)?;
    let ring_offsets = numpy_to_offsets(&ring_offsets)?;
    let geom_offsets = numpy_to_offsets(&geom_offsets)?;

    match dim {
        Dimension::XY => {
            let cb = coords_to_buffer(coords)?;
            Ok(Arc::new(geoarrow::array::PolygonArray::<i32, 2>::new(
                cb.into(),
                geom_offsets,
                ring_offsets,
                None,
                metadata,
            )))
        }
        Dimension::XYZ => {
            let cb = coords_to_buffer(coords)?;
            Ok(Arc::new(geoarrow::array::PolygonArray::<i32, 3>::new(
                cb.into(),
                geom_offsets,
                ring_offsets,
                None,
                metadata,
            )))
        }
    }
}

fn make_multipoint_arr(
    py: Python,
    coords: PyReadonlyArray2<'_, f64>,
    offsets: PyObject,
    dim: Dimension,
    metadata: Arc<ArrayMetadata>,
) -> PyGeoArrowResult<Arc<dyn NativeArray>> {
    let (geom_offsets,) = offsets.extract::<(PyReadonlyArray1<'_, i64>,)>(py)?;
    let geom_offsets = numpy_to_offsets(&geom_offsets)?;

    match dim {
        Dimension::XY => {
            let cb = coords_to_buffer(coords)?;
            Ok(Arc::new(geoarrow::array::MultiPointArray::<i32, 2>::new(
                cb.into(),
                geom_offsets,
                None,
                metadata,
            )))
        }
        Dimension::XYZ => {
            let cb = coords_to_buffer(coords)?;
            Ok(Arc::new(geoarrow::array::MultiPointArray::<i32, 3>::new(
                cb.into(),
                geom_offsets,
                None,
                metadata,
            )))
        }
    }
}

fn make_multilinestring_arr(
    py: Python,
    coords: PyReadonlyArray2<'_, f64>,
    offsets: PyObject,
    dim: Dimension,
    metadata: Arc<ArrayMetadata>,
) -> PyGeoArrowResult<Arc<dyn NativeArray>> {
    let (ring_offsets, geom_offsets) =
        offsets.extract::<(PyReadonlyArray1<'_, i64>, PyReadonlyArray1<'_, i64>)>(py)?;
    let ring_offsets = numpy_to_offsets(&ring_offsets)?;
    let geom_offsets = numpy_to_offsets(&geom_offsets)?;

    match dim {
        Dimension::XY => {
            let cb = coords_to_buffer(coords)?;
            Ok(Arc::new(
                geoarrow::array::MultiLineStringArray::<i32, 2>::new(
                    cb.into(),
                    geom_offsets,
                    ring_offsets,
                    None,
                    metadata,
                ),
            ))
        }
        Dimension::XYZ => {
            let cb = coords_to_buffer(coords)?;
            Ok(Arc::new(
                geoarrow::array::MultiLineStringArray::<i32, 3>::new(
                    cb.into(),
                    geom_offsets,
                    ring_offsets,
                    None,
                    metadata,
                ),
            ))
        }
    }
}

fn make_multipolygon_arr(
    py: Python,
    coords: PyReadonlyArray2<'_, f64>,
    offsets: PyObject,
    dim: Dimension,
    metadata: Arc<ArrayMetadata>,
) -> PyGeoArrowResult<Arc<dyn NativeArray>> {
    let (ring_offsets, polygon_offsets, geom_offsets) = offsets.extract::<(
        PyReadonlyArray1<'_, i64>,
        PyReadonlyArray1<'_, i64>,
        PyReadonlyArray1<'_, i64>,
    )>(py)?;
    let ring_offsets = numpy_to_offsets(&ring_offsets)?;
    let polygon_offsets = numpy_to_offsets(&polygon_offsets)?;
    let geom_offsets = numpy_to_offsets(&geom_offsets)?;

    match dim {
        Dimension::XY => {
            let cb = coords_to_buffer(coords)?;
            Ok(Arc::new(geoarrow::array::MultiPolygonArray::<i32, 2>::new(
                cb.into(),
                geom_offsets,
                polygon_offsets,
                ring_offsets,
                None,
                metadata,
            )))
        }
        Dimension::XYZ => {
            let cb = coords_to_buffer(coords)?;
            Ok(Arc::new(geoarrow::array::MultiPolygonArray::<i32, 3>::new(
                cb.into(),
                geom_offsets,
                polygon_offsets,
                ring_offsets,
                None,
                metadata,
            )))
        }
    }
}

fn make_wkb_arr(
    py: Python,
    input: &Bound<PyAny>,
    metadata: Arc<ArrayMetadata>,
) -> PyGeoArrowResult<geoarrow::array::WKBArray<i32>> {
    let shapely_mod = import_shapely(py)?;
    let wkb_result = call_to_wkb(py, &shapely_mod, input)?;

    let mut builder = BinaryBuilder::with_capacity(wkb_result.len()?, 0);

    for item in wkb_result.iter()? {
        let x = item?.extract::<&PyBytes>()?;
        builder.append_value(x.as_bytes());
    }

    Ok(geoarrow::array::WKBArray::new(builder.finish(), metadata))
}

// TODO: add chunk_size param to from_shapely
#[allow(unused_macros)]
macro_rules! impl_chunked_from_shapely {
    ($py_chunked_struct:ty, $py_array_struct:ty) => {
        #[pymethods]
        impl $py_chunked_struct {
            #[classmethod]
            #[pyo3(signature = (input, *, chunk_size=65536, crs=None))]
            fn from_shapely(
                _cls: &Bound<PyType>,
                py: Python,
                input: &Bound<PyAny>,
                chunk_size: usize,
                crs: Option<CRS>,
            ) -> PyGeoArrowResult<Self> {
                let len = input.len()?;
                let num_chunks = (len as f64 / chunk_size as f64).ceil() as usize;
                let mut chunks = Vec::with_capacity(num_chunks);

                for chunk_idx in 0..num_chunks {
                    let slice = PySlice::new_bound(
                        py,
                        (chunk_idx * chunk_size).try_into().unwrap(),
                        ((chunk_idx + 1) * chunk_size).try_into().unwrap(),
                        1,
                    );
                    let input_slice = input.get_item(slice)?;
                    chunks.push(
                        <$py_array_struct>::from_shapely(_cls, py, &input_slice, crs.clone())?.0,
                    );
                }

                Ok(geoarrow::chunked_array::PyChunkedNativeArray::new(chunks).into())
            }
        }
    };
}
