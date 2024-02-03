use flatgeobuf::HttpFgbReader;
use http_range_client::AsyncBufferedHttpRangeClient;
use object_store::path::Path;
use object_store::ObjectStore;

use crate::error::Result;
use crate::io::flatgeobuf::object_store_reader::ObjectStoreWrapper;

pub async fn read_flatgeobuf_async<T: ObjectStore>(
    reader: T,
    location: Path,
    bbox: Option<(f64, f64, f64, f64)>,
) -> Result<()> {
    let object_store_wrapper = ObjectStoreWrapper { reader, location };
    let async_client = AsyncBufferedHttpRangeClient::with(object_store_wrapper, "");
    let reader: HttpFgbReader = HttpFgbReader::new(async_client).await.unwrap();
    let selection = if let Some((min_x, min_y, max_x, max_y)) = bbox {
        reader.select_bbox(min_x, min_y, max_x, max_y).await?
    } else {
        reader.select_all()?
    };
    todo!()
}
