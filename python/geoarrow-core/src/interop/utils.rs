use std::sync::Arc;

use geoarrow_schema::Metadata;
use pyo3_geoarrow::{PyCrs, PyEdges};

/// Create a new Metadata from optional PyCrs and PyEdges
pub(crate) fn new_metadata(crs: Option<PyCrs>, edges: Option<PyEdges>) -> Arc<Metadata> {
    let crs = crs.map(|inner| inner.into()).unwrap_or_default();
    Arc::new(Metadata::new(crs, edges.map(|e| e.into())))
}
