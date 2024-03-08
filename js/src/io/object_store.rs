//! Shims for object store on the web.

use async_trait::async_trait;
use ehttp::{fetch, fetch_async};
// use object_store::client::get::GetClientExt;
// use object_store::client::header::get_etag;
// use object_store::http::client::Client;
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartId, ObjectMeta, ObjectStore, PutMode, PutOptions,
    PutResult, Result,
};

// pub struct HTTPObjectStore {}

// #[async_trait]
// impl ObjectStore for HTTPObjectStore {
//     async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
//         fetch_async(request)
//         GetResult
//         self.client.get_opts(location, options).await
//     }
// }
