use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use http_range_client::{AsyncHttpRangeClient, Result as HTTPRangeClientResult};
use object_store::path::Path;
use object_store::ObjectStore;

pub struct ObjectStoreWrapper {
    pub location: Path,
    pub reader: Arc<dyn ObjectStore>,
    pub size: usize,
}

#[async_trait]
impl AsyncHttpRangeClient for ObjectStoreWrapper {
    /// Send a GET range request
    async fn get_range(&self, _url: &str, range: &str) -> HTTPRangeClientResult<Bytes> {
        assert!(range.starts_with("bytes="));

        let split_range = range[6..].split('-').collect::<Vec<_>>();
        let start_range = split_range[0].parse::<usize>().unwrap();

        // Add one to the range because HTTP range strings are end-inclusive (I think)
        let end_range = split_range[1].parse::<usize>().unwrap() + 1;

        // Flatgeobuf will sometimes overfetch, but not all object store backends support
        // overfetches (e.g. this errors on a LocalFileSystem)
        // See https://github.com/flatgeobuf/flatgeobuf/issues/338
        let end_range = end_range.min(self.size);

        let bytes = self
            .reader
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
            let meta = self.reader.head(&self.location).await.unwrap();
            Ok(Some(meta.size.to_string()))
        } else {
            Ok(None)
        }
    }
}
