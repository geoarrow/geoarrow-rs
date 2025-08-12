//! Integration with the [`object_store`] crate.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use http_range_client::{AsyncHttpRangeClient, Result as HTTPRangeClientResult};
use object_store::ObjectStore;
use object_store::path::Path;

/// A wrapper around an [`ObjectStore`] that implements the [`AsyncHttpRangeClient`] trait.
#[derive(Debug, Clone)]
pub struct ObjectStoreWrapper {
    store: Arc<dyn ObjectStore>,
    location: Path,
}

impl ObjectStoreWrapper {
    /// Creates a new [`ObjectStoreWrapper`] with the given store and location.
    pub fn new(store: Arc<dyn ObjectStore>, location: Path) -> Self {
        Self { store, location }
    }
}

#[async_trait]
impl AsyncHttpRangeClient for ObjectStoreWrapper {
    /// Send a GET range request
    async fn get_range(&self, _url: &str, range: &str) -> HTTPRangeClientResult<Bytes> {
        assert!(range.starts_with("bytes="));

        let split_range = range[6..].split('-').collect::<Vec<_>>();
        let start_range = split_range[0].parse::<u64>().unwrap();

        // Add one to the range because HTTP range strings are end-inclusive (I think)
        let end_range = split_range[1].parse::<u64>().unwrap() + 1;

        let bytes = self
            .store
            .get_range(&self.location, start_range..end_range)
            .await
            .unwrap();
        Ok(bytes)
    }

    /// Send a HEAD request and return response header value
    async fn head_response_header(
        &self,
        _url: &str,
        header: &str,
    ) -> HTTPRangeClientResult<Option<String>> {
        // This is a massive hack to align APIs
        if header == "content-length" {
            let meta = self.store.head(&self.location).await.unwrap();
            Ok(Some(meta.size.to_string()))
        } else {
            Ok(None)
        }
    }
}
