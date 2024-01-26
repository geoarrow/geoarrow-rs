use crate::error::PyGeoArrowResult;
use crate::interop::util::import_pyarrow;
use crate::table::GeoTable;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3::PyAny;

#[pyfunction]
#[pyo3(signature = (path_or_buffer, /, layer=None, encoding=None, columns=None, read_geometry=true, force_2d=false, skip_features=0, max_features=None, r#where=None, bbox=None, mask=None, fids=None, sql=None, sql_dialect=None, return_fids=false, batch_size=65536, **kwargs))]
pub fn from_pyogrio(
    py: Python,
    path_or_buffer: &PyAny,
    layer: Option<&PyAny>,
    encoding: Option<&PyAny>,
    columns: Option<&PyAny>,
    read_geometry: bool,
    force_2d: bool,
    skip_features: usize,
    max_features: Option<&PyAny>,
    r#where: Option<&PyAny>,
    bbox: Option<&PyAny>,
    mask: Option<&PyAny>,
    fids: Option<&PyAny>,
    sql: Option<&PyAny>,
    sql_dialect: Option<&PyAny>,
    return_fids: bool,
    batch_size: usize,
    kwargs: Option<&PyDict>,
) -> PyGeoArrowResult<GeoTable> {
    // Imports and validation
    // Import pyarrow to validate it's >=14 and will have PyCapsule interface
    let _pyarrow_mod = import_pyarrow(py)?;
    let pyogrio_mod = py.import(intern!(py, "pyogrio"))?;

    let args = (path_or_buffer,);
    let our_kwargs = PyDict::new(py);
    our_kwargs.set_item("layer", layer)?;
    our_kwargs.set_item("encoding", encoding)?;
    our_kwargs.set_item("columns", columns)?;
    our_kwargs.set_item("read_geometry", read_geometry)?;
    our_kwargs.set_item("force_2d", force_2d)?;
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

    let context_manager = pyogrio_mod.getattr(intern!(py, "raw"))?.call_method(
        intern!(py, "open_arrow"),
        args,
        Some(our_kwargs),
    )?;
    let (_meta, record_batch_reader) = context_manager
        .call_method0(intern!(py, "__enter__"))?
        .extract::<(PyObject, PyObject)>()?;

    let maybe_table =
        GeoTable::from_arrow(py.get_type::<GeoTable>(), record_batch_reader.as_ref(py));

    // If the eval threw an exception we'll pass it through to the context manager.
    // Otherwise, __exit__ is called with empty arguments (Python "None").
    // https://pyo3.rs/v0.20.2/python_from_rust.html#need-to-use-a-context-manager-from-rust
    match maybe_table {
        Ok(table) => {
            let none = py.None();
            context_manager.call_method1("__exit__", (&none, &none, &none))?;
            Ok(table)
        }
        Err(e) => {
            context_manager
                .call_method1("__exit__", (e.get_type(py), e.value(py), e.traceback(py)))?;
            Err(e.into())
        }
    }
}
