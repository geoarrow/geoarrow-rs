use std::sync::Arc;

use crate::constructors::{
    linestrings, multilinestrings, multipoints, multipolygons, points, polygons,
};
use crate::ffi::to_python::native_array_to_pyobject;
use crate::interop::shapely::utils::import_shapely;
use arrow_array::builder::BinaryBuilder;
use geoarrow::datatypes::NativeType;
use geoarrow_schema::{CoordType, Dimension, GeometryCollectionType, Metadata};
use pyo3::PyAny;
use pyo3::exceptions::PyValueError;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedBytes;
use pyo3::types::{PyDict, PyString, PyTuple};
use pyo3_geoarrow::{PyCrs, PyGeoArrowResult};

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

    let kwargs = PyDict::new(py);
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

    let kwargs = PyDict::new(py);
    kwargs.set_item("output_dimension", 2)?;
    kwargs.set_item("include_srid", false)?;
    kwargs.set_item("flavor", "iso")?;

    Ok(shapely_mod.call_method(intern!(py, "to_wkb"), args, Some(&kwargs))?)
}

#[pyfunction]
#[pyo3(signature = (input, *, crs = None, method = "wkb"))]
pub fn from_shapely(
    py: Python,
    input: &Bound<PyAny>,
    crs: Option<PyCrs>,
    method: String,
) -> PyGeoArrowResult<PyObject> {
    let numpy_mod = py.import(intern!(py, "numpy"))?;
    let shapely_mod = import_shapely(py)?;

    let kwargs = PyDict::new(py);
    match method {
        "wkb" => {
            let metadata = Arc::new(crs.map(|inner| inner.into_inner()).unwrap_or_default());

            // TODO: support 3d WKB
            let wkb_arr = make_wkb_arr(py, input, metadata)?;
            let geom_arr = geoarrow::io::wkb::from_wkb(
                &wkb_arr,
                NativeType::GeometryCollection(GeometryCollectionType::new(
                    CoordType::default_interleaved(),
                    Dimension::XY,
                    Default::default(),
                )),
                false,
            )?;
            native_array_to_pyobject(py, geom_arr)
        }
        "ragged" => {
            let (geom_type, coords, offsets) = shapely_mod
                .call_method(intern!(py, "to_ragged_array"), (input,), Some(&kwargs))?
                .extract::<(Bound<PyAny>, Bound<PyAny>, PyObject)>()?;

            let coords = numpy_mod.call_method1(
                intern!(py, "ascontiguousarray"),
                PyTuple::new(py, vec![coords])?,
            )?;

            let geometry_type_enum = shapely_mod.getattr(intern!(py, "GeometryType"))?;

            let arr = if geom_type.eq(geometry_type_enum.getattr(intern!(py, "POINT"))?)? {
                points(coords.extract()?, crs)?.into_inner().into_inner()
            } else if geom_type.eq(geometry_type_enum.getattr(intern!(py, "LINESTRING"))?)? {
                let (geom_offsets,) = offsets.extract::<(Bound<PyAny>,)>(py)?;

                linestrings(coords.extract()?, geom_offsets.extract()?, crs)?
                    .into_inner()
                    .into_inner()
            } else if geom_type.eq(geometry_type_enum.getattr(intern!(py, "POLYGON"))?)? {
                let (ring_offsets, geom_offsets) =
                    offsets.extract::<(Bound<PyAny>, Bound<PyAny>)>(py)?;

                polygons(
                    coords.extract()?,
                    geom_offsets.extract()?,
                    ring_offsets.extract()?,
                    crs,
                )?
                .into_inner()
                .into_inner()
            } else if geom_type.eq(geometry_type_enum.getattr(intern!(py, "MULTIPOINT"))?)? {
                let (geom_offsets,) = offsets.extract::<(Bound<PyAny>,)>(py)?;

                multipoints(coords.extract()?, geom_offsets.extract()?, crs)?
                    .into_inner()
                    .into_inner()
            } else if geom_type.eq(geometry_type_enum.getattr(intern!(py, "MULTILINESTRING"))?)? {
                let (ring_offsets, geom_offsets) =
                    offsets.extract::<(Bound<PyAny>, Bound<PyAny>)>(py)?;

                multilinestrings(
                    coords.extract()?,
                    geom_offsets.extract()?,
                    ring_offsets.extract()?,
                    crs,
                )?
                .into_inner()
                .into_inner()
            } else if geom_type.eq(geometry_type_enum.getattr(intern!(py, "MULTIPOLYGON"))?)? {
                let (ring_offsets, polygon_offsets, geom_offsets) =
                    offsets.extract::<(Bound<PyAny>, Bound<PyAny>, Bound<PyAny>)>(py)?;

                multipolygons(
                    coords.extract()?,
                    geom_offsets.extract()?,
                    polygon_offsets.extract()?,
                    ring_offsets.extract()?,
                    crs,
                )?
                .into_inner()
                .into_inner()
            } else {
                return Err(PyValueError::new_err(format!(
                    "unexpected geometry type from to_ragged_array {}",
                    geom_type
                ))
                .into());
            };

            native_array_to_pyobject(py, arr)
        }
    }
}

fn make_wkb_arr(
    py: Python,
    input: &Bound<PyAny>,
    metadata: Arc<Metadata>,
) -> PyGeoArrowResult<geoarrow::array::WKBArray<i32>> {
    let shapely_mod = import_shapely(py)?;
    let wkb_result = call_to_wkb(py, &shapely_mod, input)?;

    let mut builder = BinaryBuilder::with_capacity(wkb_result.len()?, 0);

    for item in wkb_result.try_iter()? {
        let buf = item?.extract::<PyBackedBytes>()?;
        builder.append_value(buf.as_ref());
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
                crs: Option<PyCrs>,
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
