pub mod sync;

use std::sync::Arc;

use crate::error::PyGeoArrowResult;
#[cfg(feature = "async")]
use object_store::{
    ClientOptions, ObjectStore, http::HttpBuilder, local::LocalFileSystem, path::Path,
};
use pyo3::pybacked::PyBackedStr;
#[cfg(feature = "async")]
use pyo3_object_store::AnyObjectStore;
use sync::FileReader;

use pyo3::prelude::*;
use url::Url;

#[cfg(feature = "async")]
#[derive(Debug, Clone)]
pub struct AsyncFileReader {
    pub store: Arc<dyn ObjectStore>,
    pub path: Path,
}

pub enum AnyFileReader {
    Sync(FileReader),
    #[cfg(feature = "async")]
    Async(AsyncFileReader),
}

/// Construct a reader for the user that will always be asynchronous
///
/// object_store default instances will be created for local and HTTP(s) files.
#[allow(dead_code)]
#[cfg(feature = "async")]
pub fn construct_async_reader(
    file: Bound<PyAny>,
    store: Option<Bound<PyAny>>,
) -> PyGeoArrowResult<AsyncFileReader> {
    // If the user passed an object store instance, use that

    use pyo3_object_store::AnyObjectStore;
    if let Some(store) = store {
        let async_reader = AsyncFileReader {
            store: store.extract::<AnyObjectStore>()?.into(),
            path: file.extract::<String>()?.into(),
        };
        return Ok(async_reader);
    }

    // HTTP(s) url
    let path_or_url = file.extract::<PyBackedStr>()?;
    if path_or_url.starts_with("http") {
        return default_http_store(&path_or_url);
    }

    // Make default local store
    // Note: not sure if this works with relative paths
    default_local_store(&path_or_url)
}

/// Construct a reader for the user that can be either synchronous or asynchronous
///
/// If the user has not passed in an object store instance but the `file` points to a http address,
/// an HTTPStore will be created for it.
pub fn construct_reader(
    file: Bound<PyAny>,
    store: Option<Bound<PyAny>>,
) -> PyGeoArrowResult<AnyFileReader> {
    // If the user passed an object store instance, use that
    #[cfg(feature = "async")]
    if let Some(store) = store {
        let async_reader = AsyncFileReader {
            store: store.extract::<AnyObjectStore>()?.into(),
            path: file.extract::<String>()?.into(),
        };
        return Ok(AnyFileReader::Async(async_reader));
    }

    // If the user's path is a "known" URL (i.e. http(s)) then construct an object store
    // instance for them.
    #[cfg(feature = "async")]
    if let Ok(path_or_url) = file.extract::<PyBackedStr>() {
        if path_or_url.starts_with("http") {
            return Ok(AnyFileReader::Async(default_http_store(&path_or_url)?));
        }
    }

    Ok(AnyFileReader::Sync(file.extract()?))
}

#[allow(dead_code)]
#[cfg(feature = "async")]
fn default_http_store(path_or_url: &str) -> PyGeoArrowResult<AsyncFileReader> {
    let url = Url::parse(path_or_url)?;

    let store_input = format!("{}://{}", url.scheme(), url.domain().unwrap());

    let options = ClientOptions::new().with_allow_http(true);
    let store = HttpBuilder::new()
        .with_url(store_input)
        .with_client_options(options)
        .build()?;
    let path = url.path().trim_start_matches('/');

    let async_reader = AsyncFileReader {
        store: Arc::new(store),
        path: path.into(),
    };
    Ok(async_reader)
}

#[cfg(feature = "async")]
fn default_local_store(path: &str) -> PyGeoArrowResult<AsyncFileReader> {
    let async_reader = AsyncFileReader {
        store: Arc::new(LocalFileSystem::new()),
        path: path.into(),
    };
    Ok(async_reader)
}
