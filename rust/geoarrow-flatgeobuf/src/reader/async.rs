use std::pin::Pin;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, Schema, SchemaRef};
use async_stream::try_stream;
use flatgeobuf::AsyncFeatureIter;
use futures::Stream;
use futures::stream::BoxStream;
use futures::task::{Context, Poll};
use geoarrow_schema::GeoArrowType;
use geoarrow_schema::error::{GeoArrowError, GeoArrowResult};
use geozero::FeatureProperties;
use http_range_client::AsyncHttpRangeClient;

use crate::reader::FlatGeobufReaderOptions;
use crate::reader::common::parse_header;
use crate::reader::table_builder::{GeoArrowRecordBatchBuilder, GeoArrowRecordBatchBuilderOptions};

struct FlatGeobufRecordBatchStreamInner<T: AsyncHttpRangeClient> {
    selection: AsyncFeatureIter<T>,
    geometry_type: GeoArrowType,
    batch_size: usize,
    properties_schema: SchemaRef,
    num_rows_remaining: Option<usize>,
    read_geometry: bool,
}

impl<T: AsyncHttpRangeClient + Unpin + Send + 'static> FlatGeobufRecordBatchStreamInner<T> {
    fn try_new(
        selection: AsyncFeatureIter<T>,
        options: FlatGeobufReaderOptions,
    ) -> GeoArrowResult<Self> {
        let (geometry_type, properties_schema) = parse_header(
            selection.header(),
            options.coord_type,
            options.prefer_view_types,
            options.columns.as_ref(),
        )?;
        let num_rows_remaining = selection.features_count();
        Ok(Self {
            selection,
            geometry_type,
            batch_size: options.batch_size,
            properties_schema,
            num_rows_remaining,
            read_geometry: options.read_geometry,
        })
    }

    fn output_schema(&self) -> SchemaRef {
        let mut fields = self.properties_schema.fields().to_vec();
        if self.read_geometry {
            fields.push(self.geometry_type.to_field("geometry", true).into());
        }
        Arc::new(Schema::new_with_metadata(
            fields,
            self.properties_schema.metadata().clone(),
        ))
    }

    async fn process_batch(&mut self) -> GeoArrowResult<Option<RecordBatch>> {
        let options = GeoArrowRecordBatchBuilderOptions {
            batch_size: self
                .num_rows_remaining
                .map(|num_rows_remaining| num_rows_remaining.min(self.batch_size)),
            error_on_extra_columns: false,
            read_geometry: self.read_geometry,
        };
        let mut record_batch_builder = GeoArrowRecordBatchBuilder::new(
            self.properties_schema.clone(),
            self.geometry_type.clone(),
            &options,
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

                record_batch_builder.push_geometry(
                    feature
                        .geometry_trait()
                        .map_err(|err| GeoArrowError::External(Box::new(err)))?
                        .as_ref(),
                )?;

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

impl FlatGeobufRecordBatchStream {
    pub fn try_new(
        selection: AsyncFeatureIter<impl AsyncHttpRangeClient + Unpin + Send + 'static>,
        options: FlatGeobufReaderOptions,
    ) -> GeoArrowResult<Self> {
        let inner = FlatGeobufRecordBatchStreamInner::try_new(selection, options)?;
        Ok(inner.into_stream())
    }
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

    use flatgeobuf::HttpFgbReader;
    use futures::TryStreamExt;
    use http_range_client::AsyncBufferedHttpRangeClient;
    use object_store::ObjectStore;
    use object_store::local::LocalFileSystem;

    use crate::reader::object_store::ObjectStoreWrapper;

    use super::*;

    fn fixtures_dir() -> Arc<dyn ObjectStore> {
        Arc::new(
            LocalFileSystem::new_with_prefix(
                current_dir().unwrap().parent().unwrap().parent().unwrap(),
            )
            .unwrap(),
        )
    }

    async fn new_from_store(
        store: Arc<dyn ObjectStore>,
        location: object_store::path::Path,
    ) -> flatgeobuf::Result<HttpFgbReader<ObjectStoreWrapper>> {
        let object_store_wrapper = ObjectStoreWrapper::new(store, location);
        let async_client = AsyncBufferedHttpRangeClient::with(object_store_wrapper, "");
        HttpFgbReader::new(async_client).await
    }

    #[tokio::test]
    async fn test_countries() {
        let store = fixtures_dir();
        let fgb_reader = new_from_store(store, "fixtures/flatgeobuf/countries.fgb".into())
            .await
            .unwrap();
        let selection = fgb_reader.select_all().await.unwrap();
        let stream = FlatGeobufRecordBatchStream::try_new(selection, Default::default()).unwrap();
        let _schema = stream.schema();
        let batches = stream.try_collect::<Vec<_>>().await.unwrap();

        let num_rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
        assert_eq!(num_rows, 179);
    }

    #[tokio::test]
    async fn test_countries_bbox() {
        let store = fixtures_dir();
        let fgb_reader = new_from_store(store, "fixtures/flatgeobuf/countries.fgb".into())
            .await
            .unwrap();
        let selection = fgb_reader.select_bbox(0., -90., 180., 90.).await.unwrap();
        let stream = FlatGeobufRecordBatchStream::try_new(selection, Default::default()).unwrap();
        let _schema = stream.schema();
        let batches = stream.try_collect::<Vec<_>>().await.unwrap();

        let num_rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
        assert_eq!(num_rows, 133);
    }

    #[tokio::test]
    async fn test_nz_buildings() {
        let store = fixtures_dir();
        let fgb_reader = new_from_store(
            store,
            "fixtures/flatgeobuf/nz-building-outlines-small.fgb".into(),
        )
        .await
        .unwrap();
        let selection = fgb_reader.select_all().await.unwrap();
        let stream = FlatGeobufRecordBatchStream::try_new(selection, Default::default()).unwrap();
        let _schema = stream.schema();
        let batches = stream.try_collect::<Vec<_>>().await.unwrap();

        let num_rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
        assert_eq!(num_rows, 2000);
    }
}
