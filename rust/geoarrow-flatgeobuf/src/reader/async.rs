use std::pin::Pin;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, Schema, SchemaRef};
use async_stream::try_stream;
use flatgeobuf::{AsyncFeatureIter, HttpFgbReader};
use futures::Stream;
use futures::stream::BoxStream;
use futures::task::{Context, Poll};
use geoarrow_schema::error::{GeoArrowError, GeoArrowResult};
use geoarrow_schema::{CoordType, GeoArrowType};
use geozero::FeatureProperties;
use http_range_client::{AsyncBufferedHttpRangeClient, AsyncHttpRangeClient};

use crate::reader::FlatGeobufReaderOptions;
use crate::reader::common::parse_header;
use crate::reader::table_builder::GeoArrowRecordBatchBuilder;

/// The primary async entry point for reading FlatGeobuf files as a stream of record batches.
pub struct FlatGeobufStreamBuilder<T: AsyncHttpRangeClient + Unpin + Send + 'static> {
    reader: HttpFgbReader<T>,
}

impl<T: AsyncHttpRangeClient + Unpin + Send + 'static> FlatGeobufStreamBuilder<T> {
    /// Create a new [FlatGeobufStreamBuilder] from an [AsyncBufferedHttpRangeClient]
    pub async fn open(reader: AsyncBufferedHttpRangeClient<T>) -> GeoArrowResult<Self> {
        let reader = HttpFgbReader::new(reader).await.unwrap();
        Ok(Self { reader })
    }

    /// Create a new [FlatGeobufStreamBuilder] directly from a client.
    pub async fn new_from_client(reader: T, url: &str) -> GeoArrowResult<Self> {
        let client = AsyncBufferedHttpRangeClient::with(reader, url);
        Self::open(client).await
    }

    // TODO: deduplicate with `output_schema`
    pub fn output_schema(
        &self,
        coord_type: CoordType,
        prefer_view_types: bool,
    ) -> GeoArrowResult<Schema> {
        let (geometry_type, properties_schema) =
            parse_header(self.reader.header(), coord_type, prefer_view_types)?;

        let mut fields = properties_schema.fields().to_vec();
        fields.push(geometry_type.to_field("geometry", true).into());
        Ok(Schema::new_with_metadata(
            fields,
            properties_schema.metadata().clone(),
        ))
    }

    /// Read from the FlatGeobuf file
    pub async fn read(
        self,
        options: FlatGeobufReaderOptions,
    ) -> GeoArrowResult<FlatGeobufRecordBatchStream> {
        let (geometry_type, properties_schema) = parse_header(
            self.reader.header(),
            options.coord_type,
            options.prefer_view_types,
        )?;
        let selection = if let Some((min_x, min_y, max_x, max_y)) = options.bbox {
            self.reader.select_bbox(min_x, min_y, max_x, max_y).await
        } else {
            self.reader.select_all().await
        }
        .map_err(|err| GeoArrowError::External(Box::new(err)))?;

        let num_rows = selection.features_count();

        let inner_stream = FlatGeobufRecordBatchStreamInner {
            selection,
            geometry_type,
            batch_size: options.batch_size.unwrap_or(65_536),
            properties_schema,
            num_rows_remaining: num_rows,
        };
        Ok(inner_stream.into_stream())
    }
}

#[cfg(feature = "object_store")]
impl FlatGeobufStreamBuilder<super::object_store_reader::ObjectStoreWrapper> {
    /// Create a [FlatGeobufStreamBuilder] from an ObjectStore instance.
    pub async fn new_from_store(
        store: Arc<dyn object_store::ObjectStore>,
        location: object_store::path::Path,
    ) -> GeoArrowResult<Self> {
        let head = store
            .head(&location)
            .await
            .map_err(|err| GeoArrowError::External(Box::new(err)))?;
        let object_store_wrapper = super::object_store_reader::ObjectStoreWrapper {
            reader: store,
            location,
            size: head.size,
        };
        let async_client = AsyncBufferedHttpRangeClient::with(object_store_wrapper, "");
        Self::open(async_client).await
    }
}

struct FlatGeobufRecordBatchStreamInner<T: AsyncHttpRangeClient> {
    selection: AsyncFeatureIter<T>,
    geometry_type: GeoArrowType,
    batch_size: usize,
    properties_schema: SchemaRef,
    num_rows_remaining: Option<usize>,
    // pending_fut: Option<BoxFuture<'static, GeoArrowResult<Option<RecordBatch>>>>,
}

impl<T: AsyncHttpRangeClient + Unpin + Send + 'static> FlatGeobufRecordBatchStreamInner<T> {
    fn output_schema(&self) -> SchemaRef {
        let mut fields = self.properties_schema.fields().to_vec();
        fields.push(self.geometry_type.to_field("geometry", true).into());
        Arc::new(Schema::new_with_metadata(
            fields,
            self.properties_schema.metadata().clone(),
        ))
    }

    async fn process_batch(&mut self) -> GeoArrowResult<Option<RecordBatch>> {
        let batch_size = self
            .num_rows_remaining
            .map(|num_rows_remaining| num_rows_remaining.min(self.batch_size));
        let mut record_batch_builder = GeoArrowRecordBatchBuilder::new(
            self.properties_schema.clone(),
            self.geometry_type.clone(),
            batch_size,
        );

        let mut row_count = 0;
        loop {
            if row_count >= self.batch_size {
                let batch = record_batch_builder.finish()?;
                return Ok(Some(batch));
            }

            if let Some(feature) = self
                .selection
                .next()
                .await
                .map_err(|err| GeoArrowError::External(Box::new(err)))?
            {
                feature
                    .process_properties(&mut record_batch_builder)
                    .map_err(|err| GeoArrowError::External(Box::new(err)))?;
                // record_batch_builder.properties_end()?;

                record_batch_builder.push_geometry(
                    feature
                        .geometry_trait()
                        .map_err(|err| GeoArrowError::External(Box::new(err)))?
                        .as_ref(),
                )?;

                // $builder.feature_end(0)?;
                row_count += 1;
            } else if row_count > 0 {
                return Ok(Some(record_batch_builder.finish()?));
            } else {
                return Ok(None);
            }
        }
    }

    fn into_stream(mut self) -> FlatGeobufRecordBatchStream {
        let schema = self.output_schema();
        let stream = Box::pin(try_stream! {
            loop {
                let maybe_batch = self.process_batch().await.map_err(|e| {
                    ArrowError::ExternalError(Box::new(e))
                })?;

                if let Some(batch) = maybe_batch {
                    yield batch;
                } else {
                    break;
                }
            }
        });
        FlatGeobufRecordBatchStream { stream, schema }
    }
}

pub struct FlatGeobufRecordBatchStream {
    stream: BoxStream<'static, Result<RecordBatch, ArrowError>>,
    schema: SchemaRef,
}

impl Stream for FlatGeobufRecordBatchStream {
    type Item = Result<RecordBatch, ArrowError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.stream).poll_next(cx)
    }
}

impl FlatGeobufRecordBatchStream {
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod test {
    use std::env::current_dir;

    use futures::TryStreamExt;
    use object_store::ObjectStore;
    use object_store::local::LocalFileSystem;

    use super::*;

    fn fixtures_dir() -> Arc<dyn ObjectStore> {
        Arc::new(
            LocalFileSystem::new_with_prefix(
                current_dir().unwrap().parent().unwrap().parent().unwrap(),
            )
            .unwrap(),
        )
    }

    #[tokio::test]
    async fn test_countries() {
        let store = fixtures_dir();
        let builder = FlatGeobufStreamBuilder::new_from_store(
            store,
            "fixtures/flatgeobuf/countries.fgb".into(),
        )
        .await
        .unwrap();
        let reader = builder.read(Default::default()).await.unwrap();
        let _schema = reader.schema();
        let batches = reader.try_collect::<Vec<_>>().await.unwrap();

        let num_rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
        assert_eq!(num_rows, 179);
    }

    #[tokio::test]
    async fn test_countries_bbox() {
        let store = fixtures_dir();
        let builder = FlatGeobufStreamBuilder::new_from_store(
            store,
            "fixtures/flatgeobuf/countries.fgb".into(),
        )
        .await
        .unwrap();
        let options = FlatGeobufReaderOptions {
            bbox: Some((0., -90., 180., 90.)),
            ..Default::default()
        };
        let reader = builder.read(options).await.unwrap();
        let _schema = reader.schema();
        let batches = reader.try_collect::<Vec<_>>().await.unwrap();

        let num_rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
        assert_eq!(num_rows, 133);
    }

    // #[tokio::test]
    // async fn test_nz_buildings() {
    //     let fs = Arc::new(LocalFileSystem::new_with_prefix(current_dir().unwrap()).unwrap());
    //     let options = FlatGeobufReaderOptions::default();
    //     let _table = read_flatgeobuf_async(
    //         fs,
    //         Path::from("fixtures/flatgeobuf/nz-building-outlines-small.fgb"),
    //         options,
    //     )
    //     .await
    //     .unwrap();
    // }
}
