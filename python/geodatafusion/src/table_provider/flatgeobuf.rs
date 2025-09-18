use datafusion::catalog::TableProvider;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::prelude::SessionContext;
use datafusion_ffi::table_provider::FFI_TableProvider;
use geodatafusion_flatgeobuf::FlatGeobufFormat;
use pyo3::prelude::*;
use pyo3::types::PyCapsule;
use pyo3::{Bound, PyResult, Python, pyclass, pymethods};
use pyo3_async_runtimes::tokio::get_runtime;
use std::sync::Arc;

#[pyfunction]
pub(crate) fn new_flatgeobuf(path: &str) -> PyFlatGeobufTableProvider {
    let format = Arc::new(FlatGeobufFormat::default());

    let options = ListingOptions::new(format).with_file_extension(".fgb");

    let table_path = ListingTableUrl::parse(path).unwrap();

    let state = SessionContext::new().state();
    let runtime = get_runtime();
    let inferred_schema =
        runtime.block_on(async { options.infer_schema(&state, &table_path).await.unwrap() });

    let config = ListingTableConfig::new(table_path)
        .with_listing_options(options)
        .with_schema(inferred_schema);

    let table = ListingTable::try_new(config).unwrap();
    PyFlatGeobufTableProvider(Arc::new(table))
}

#[pyclass(module = "geodatafusion", name = "FlatGeobufTableProvider", frozen)]
pub(crate) struct PyFlatGeobufTableProvider(Arc<dyn TableProvider + Send>);

#[pymethods]
impl PyFlatGeobufTableProvider {
    pub fn __datafusion_table_provider__<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let name = cr"datafusion_table_provider".into();

        let provider = FFI_TableProvider::new(self.0.clone(), false, None);

        PyCapsule::new(py, provider, Some(name))
    }
}
