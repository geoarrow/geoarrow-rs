use crate::error::PyGeoArrowResult;
use crate::interop::util::import_pyogrio;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3::PyAny;
use pyo3_arrow::PyTable;

#[allow(clippy::too_many_arguments)]
#[pyfunction]
#[pyo3(signature = (path_or_buffer, /, layer=None, encoding=None, columns=None, read_geometry=true, skip_features=0, max_features=None, r#where=None, bbox=None, mask=None, fids=None, sql=None, sql_dialect=None, return_fids=false, batch_size=65536, **kwargs))]
pub fn read_pyogrio(
    py: Python,
    path_or_buffer: &Bound<PyAny>,
    layer: Option<&Bound<PyAny>>,
    encoding: Option<&Bound<PyAny>>,
    columns: Option<&Bound<PyAny>>,
    read_geometry: bool,
    skip_features: usize,
    max_features: Option<&Bound<PyAny>>,
    r#where: Option<&Bound<PyAny>>,
    bbox: Option<&Bound<PyAny>>,
    mask: Option<&Bound<PyAny>>,
    fids: Option<&Bound<PyAny>>,
    sql: Option<&Bound<PyAny>>,
    sql_dialect: Option<&Bound<PyAny>>,
    return_fids: bool,
    batch_size: usize,
    kwargs: Option<&Bound<PyDict>>,
) -> PyGeoArrowResult<PyObject> {
    let pyogrio_mod = import_pyogrio(py)?;

    let args = (path_or_buffer,);
    let our_kwargs = PyDict::new_bound(py);
    our_kwargs.set_item("layer", layer)?;
    our_kwargs.set_item("encoding", encoding)?;
    our_kwargs.set_item("columns", columns)?;
    our_kwargs.set_item("read_geometry", read_geometry)?;
    // NOTE: We always read only 2D data for now.
    // Edit: ValueError: forcing 2D is not supported for Arrow
    // our_kwargs.set_item("force_2d", true)?;
    our_kwargs.set_item("skip_features", skip_features)?;
    our_kwargs.set_item("max_features", max_features)?;
    our_kwargs.set_item("where", r#where)?;
    our_kwargs.set_item("bbox", bbox)?;
    our_kwargs.set_item("mask", mask)?;
    our_kwargs.set_item("fids", fids)?;
    our_kwargs.set_item("sql", sql)?;
    our_kwargs.set_item("sql_dialect", sql_dialect)?;
    our_kwargs.set_item("return_fids", return_fids)?;
    our_kwargs.set_item("batch_size", batch_size)?;
    if let Some(kwargs) = kwargs {
        our_kwargs.update(kwargs.as_mapping())?;
    }
    our_kwargs.set_item("use_pyarrow", false)?;

    let context_manager =
        pyogrio_mod.call_method(intern!(py, "open_arrow"), args, Some(&our_kwargs))?;
    let (_meta, record_batch_reader) = context_manager
        .call_method0(intern!(py, "__enter__"))?
        .extract::<(PyObject, PyObject)>()?;

    let maybe_table = record_batch_reader.bind(py).extract::<PyTable>();

    // If the eval threw an exception we'll pass it through to the context manager.
    // Otherwise, __exit__ is called with empty arguments (Python "None").
    // https://pyo3.rs/v0.20.2/python_from_rust.html#need-to-use-a-context-manager-from-rust
    match maybe_table {
        Ok(table) => {
            let none = py.None();
            context_manager.call_method1("__exit__", (&none, &none, &none))?;
            Ok(table.to_arro3(py)?)
        }
        Err(e) => {
            let py_err = PyErr::from(e);
            context_manager.call_method1(
                "__exit__",
                (
                    py_err.get_type_bound(py),
                    py_err.value_bound(py),
                    py_err.traceback_bound(py),
                ),
            )?;
            Err(py_err.into())
        }
    }
}
