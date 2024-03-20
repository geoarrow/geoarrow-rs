//! Shims for object store on the web.

use async_trait::async_trait;
use std::fmt;
use std::sync::Arc;
// use object_store::client::get::GetClientExt;
// use object_store::client::header::get_etag;
// use object_store::http::client::Client;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, GetResultPayload, ListResult, MultipartId, ObjectMeta, ObjectStore,
    PutMode, PutOptions, PutResult, Result,
};
use reqwest::Client;
use tokio::io::AsyncWrite;

#[derive(Debug)]
pub struct HTTPWasmStore {
    client: Arc<Client>,
}

#[async_trait]
impl ObjectStore for HTTPWasmStore {
    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        todo!()
    }

    async fn put_opts(
        &self,
        _location: &Path,
        _bytes: Bytes,
        _opts: PutOptions,
    ) -> Result<PutResult> {
        Err(object_store::Error::NotImplemented)
    }

    async fn put_multipart(
        &self,
        _location: &Path,
    ) -> Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        Err(object_store::Error::NotImplemented)
    }

    fn list(&self, _prefix: Option<&Path>) -> BoxStream<'_, Result<ObjectMeta>> {
        todo!()
        // let prefix_len = prefix.map(|p| p.as_ref().len()).unwrap_or_default();
        // let prefix = prefix.cloned();
        // futures::stream::once(async move {
        //     let status = self.client.list(prefix.as_ref(), "infinity").await?;

        //     let iter = status
        //         .response
        //         .into_iter()
        //         .filter(|r| !r.is_dir())
        //         .map(|response| {
        //             response.check_ok()?;
        //             response.object_meta(self.client.base_url())
        //         })
        //         // Filter out exact prefix matches
        //         .filter_ok(move |r| r.location.as_ref().len() > prefix_len);

        //     Ok::<_, object_store::Error>(futures::stream::iter(iter))
        // })
        // .try_flatten()
        // .boxed()
    }

    async fn abort_multipart(&self, _location: &Path, _multipart_id: &MultipartId) -> Result<()> {
        Err(object_store::Error::NotImplemented)
    }

    async fn delete(&self, _location: &Path) -> Result<()> {
        Err(object_store::Error::NotImplemented)
    }

    async fn list_with_delimiter(&self, _prefix: Option<&Path>) -> Result<ListResult> {
        Err(object_store::Error::NotImplemented)
    }

    async fn copy(&self, _from: &Path, _to: &Path) -> Result<()> {
        Err(object_store::Error::NotImplemented)
    }

    async fn copy_if_not_exists(&self, _from: &Path, _to: &Path) -> Result<()> {
        Err(object_store::Error::NotImplemented)
    }
}

impl fmt::Display for HTTPWasmStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "HTTPWasmStore")
    }
}
