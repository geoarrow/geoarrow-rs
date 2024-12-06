pub mod sync;

use std::sync::Arc;

use crate::error::PyGeoArrowResult;
#[cfg(feature = "async")]
use object_store::http::HttpBuilder;
#[cfg(feature = "async")]
use object_store::path::Path;
#[cfg(feature = "async")]
use object_store::{ClientOptions, ObjectStore};
#[cfg(feature = "async")]
use pyo3_object_store::PyObjectStore;
use sync::FileReader;

use pyo3::prelude::*;
use url::Url;

#[cfg(feature = "async")]
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
pub fn construct_async_reader(
    file: Bound<PyAny>,
    store: Option<Bound<PyAny>>,
) -> PyGeoArrowResult<AsyncFileReader> {
    todo!()
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
            store: store.extract::<PyObjectStore>()?.into_inner(),
            path: file.extract::<String>()?.into(),
        };
        return Ok(AnyFileReader::Async(async_reader));
    }

    // If the user's path is a "known" URL (i.e. http(s)) then construct an object store
    // instance for them.
    #[cfg(feature = "async")]
    if let Ok(path_or_url) = file.extract::<String>() {
        if path_or_url.starts_with("http") {
            let url = Url::parse(&path_or_url)?;
            // Expecting that the url input is something like
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
            return Ok(AnyFileReader::Async(async_reader));
        }
    }

    Ok(AnyFileReader::Sync(file.extract()?))
}
