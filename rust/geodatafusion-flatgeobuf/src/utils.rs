use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use flatgeobuf::HttpFgbReader;
use geoarrow_flatgeobuf::reader::object_store::ObjectStoreWrapper;
use http_range_client::AsyncBufferedHttpRangeClient;
use object_store::ObjectStore;
use object_store::path::Path;

pub(crate) async fn open_flatgeobuf_reader(
    store: Arc<dyn ObjectStore>,
    location: Path,
) -> Result<HttpFgbReader<ObjectStoreWrapper>> {
    let object_store_wrapper = ObjectStoreWrapper::new(store, location);
    let async_client = AsyncBufferedHttpRangeClient::with(object_store_wrapper, "");
    HttpFgbReader::new(async_client)
        .await
        .map_err(|err| DataFusionError::External(Box::new(err)))
}
