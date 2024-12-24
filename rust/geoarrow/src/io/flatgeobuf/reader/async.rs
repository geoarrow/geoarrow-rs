use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use flatgeobuf::{AsyncFeatureIter, HttpFgbReader};
use futures::task::{Context, Poll};
use futures::Stream;
use geozero::{FeatureProcessor, FeatureProperties};
use http_range_client::{AsyncBufferedHttpRangeClient, AsyncHttpRangeClient};
use object_store::path::Path;
use object_store::ObjectStore;

use crate::array::metadata::ArrayMetadata;
use crate::array::*;
use crate::datatypes::{Dimension, NativeType};
use crate::error::{GeoArrowError, Result};
use crate::io::flatgeobuf::reader::common::{infer_from_header, FlatGeobufReaderOptions};
use crate::io::flatgeobuf::reader::object_store_reader::ObjectStoreWrapper;
use crate::io::geozero::array::GeometryStreamBuilder;
use crate::io::geozero::table::{GeoTableBuilder, GeoTableBuilderOptions};

/// A builder for [FlatGeobufReader]
pub struct FlatGeobufStreamBuilder<T: AsyncHttpRangeClient> {
    reader: HttpFgbReader<T>,
}

impl<T: AsyncHttpRangeClient> FlatGeobufStreamBuilder<T> {
    /// Create a new [FlatGeobufStreamBuilder] from an [AsyncBufferedHttpRangeClient]
    pub async fn new(reader: AsyncBufferedHttpRangeClient<T>) -> Result<Self> {
        let reader = HttpFgbReader::new(reader).await.unwrap();
        Ok(Self { reader })
    }

    /// Create a new [FlatGeobufStreamBuilder] directly from a client.
    pub async fn new_from_client(reader: T, url: &str) -> Result<Self> {
        let client = AsyncBufferedHttpRangeClient::with(reader, url);
        Self::new(client).await
    }

    /// Read from the FlatGeobuf file
    pub async fn read(self, options: FlatGeobufReaderOptions) -> Result<FlatGeobufStreamReader<T>> {
        let (data_type, properties_schema, array_metadata) =
            infer_from_header(self.reader.header())?;
        if let Some((min_x, min_y, max_x, max_y)) = options.bbox {
            let selection = self.reader.select_bbox(min_x, min_y, max_x, max_y).await?;
            let num_rows = selection.features_count();
            Ok(FlatGeobufStreamReader {
                selection,
                data_type,
                batch_size: options.batch_size.unwrap_or(65_536),
                properties_schema,
                num_rows_remaining: num_rows,
                array_metadata,
            })
        } else {
            let selection = self.reader.select_all().await?;
            let num_rows = selection.features_count();
            Ok(FlatGeobufStreamReader {
                selection,
                data_type,
                batch_size: options.batch_size.unwrap_or(65_536),
                properties_schema,
                num_rows_remaining: num_rows,
                array_metadata,
            })
        }
    }
}

impl FlatGeobufStreamBuilder<ObjectStoreWrapper> {
    /// Create a [FlatGeobufStreamBuilder] from an [ObjectStore] instance.
    pub async fn new_from_store(store: Arc<dyn ObjectStore>, location: Path) -> Result<Self> {
        let head = store.head(&location).await?;
        let object_store_wrapper = ObjectStoreWrapper {
            reader: store,
            location,
            size: head.size,
        };
        let async_client = AsyncBufferedHttpRangeClient::with(object_store_wrapper, "");
        Self::new(async_client).await
    }
}

/// An iterator over record batches from a FlatGeobuf file.
///
/// This implements [arrow_array::RecordBatchReader], which you can use to access data.
pub struct FlatGeobufStreamReader<T: AsyncHttpRangeClient> {
    selection: AsyncFeatureIter<T>,
    data_type: NativeType,
    batch_size: usize,
    properties_schema: SchemaRef,
    num_rows_remaining: Option<usize>,
    array_metadata: Arc<ArrayMetadata>,
}

impl<T: AsyncHttpRangeClient> FlatGeobufStreamReader<T> {
    /// Access the schema of the batches emitted from this stream.
    pub fn schema(&self) -> SchemaRef {
        let geom_field =
            self.data_type
                .to_field_with_metadata("geometry", true, &self.array_metadata);
        let mut fields = self.properties_schema.fields().to_vec();
        fields.push(Arc::new(geom_field));
        Arc::new(Schema::new_with_metadata(
            fields,
            self.properties_schema.metadata().clone(),
        ))
    }

    fn construct_options(&self) -> GeoTableBuilderOptions {
        let coord_type = self.data_type.coord_type();
        let mut batch_size = self.batch_size;
        if let Some(num_rows_remaining) = self.num_rows_remaining {
            batch_size = batch_size.min(num_rows_remaining);
        }
        GeoTableBuilderOptions::new(
            coord_type,
            false,
            Some(batch_size),
            Some(self.properties_schema.clone()),
            self.num_rows_remaining,
            self.array_metadata.clone(),
        )
    }

    async fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        let options = self.construct_options();
        let batch_size = options.batch_size;

        macro_rules! impl_read {
            ($builder:expr) => {{
                let mut row_count = 0;
                loop {
                    if row_count >= batch_size {
                        let (batches, _schema) = $builder.finish()?.into_inner();
                        assert_eq!(batches.len(), 1);
                        return Ok(Some(batches.into_iter().next().unwrap()));
                    }

                    if let Some(feature) = self.selection.next().await? {
                        feature.process_properties(&mut $builder)?;
                        $builder.properties_end()?;

                        $builder.push_geometry(feature.geometry_trait()?.as_ref())?;

                        $builder.feature_end(0)?;
                        row_count += 1;
                    } else {
                        return Ok(None);
                    }
                }
            }};
        }

        match self.data_type {
            NativeType::Point(_, dim) => {
                let mut builder = GeoTableBuilder::<PointBuilder>::new_with_options(dim, options);
                impl_read!(builder)
            }
            NativeType::LineString(_, dim) => {
                let mut builder =
                    GeoTableBuilder::<LineStringBuilder>::new_with_options(dim, options);
                impl_read!(builder)
            }
            NativeType::Polygon(_, dim) => {
                let mut builder = GeoTableBuilder::<PolygonBuilder>::new_with_options(dim, options);
                impl_read!(builder)
            }
            NativeType::MultiPoint(_, dim) => {
                let mut builder =
                    GeoTableBuilder::<MultiPointBuilder>::new_with_options(dim, options);
                impl_read!(builder)
            }
            NativeType::MultiLineString(_, dim) => {
                let mut builder =
                    GeoTableBuilder::<MultiLineStringBuilder>::new_with_options(dim, options);
                impl_read!(builder)
            }
            NativeType::MultiPolygon(_, dim) => {
                let mut builder =
                    GeoTableBuilder::<MultiPolygonBuilder>::new_with_options(dim, options);
                impl_read!(builder)
            }
            NativeType::Geometry(_) | NativeType::GeometryCollection(_, _) => {
                let mut builder = GeoTableBuilder::<GeometryStreamBuilder>::new_with_options(
                    // TODO: I think this is unused? remove.
                    Dimension::XY,
                    options,
                );
                impl_read!(builder)
            }
            geom_type => Err(GeoArrowError::NotYetImplemented(format!(
                "Parsing FlatGeobuf from {:?} geometry type not yet supported",
                geom_type
            ))),
        }
    }
}

impl<T> Stream for FlatGeobufStreamReader<T>
where
    T: AsyncHttpRangeClient + Unpin + Send,
{
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let future = self.next_batch();
        futures::pin_mut!(future);

        match future.poll(cx) {
            Poll::Ready(Ok(Some(feature))) => Poll::Ready(Some(Ok(feature))),
            Poll::Ready(Err(err)) => Poll::Ready(Some(Err(err))),
            // End of stream
            Poll::Ready(Ok(None)) => Poll::Ready(None),
            // Still waiting
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod test {
    use std::env::current_dir;

    use crate::table::Table;

    use super::*;
    use futures::TryStreamExt;
    use object_store::local::LocalFileSystem;

    #[tokio::test]
    async fn test_countries() {
        let store = Arc::new(LocalFileSystem::new_with_prefix(current_dir().unwrap()).unwrap());
        let options = FlatGeobufReaderOptions::default();
        let builder = FlatGeobufStreamBuilder::new_from_store(
            store,
            Path::from("fixtures/flatgeobuf/countries.fgb"),
        )
        .await
        .unwrap();
        let reader = builder.read(options).await.unwrap();
        let schema = reader.schema();
        let batches = reader.try_collect::<Vec<_>>().await.unwrap();
        let table = Table::try_new(batches, schema).unwrap();
        assert_eq!(table.len(), 179);
    }

    // #[tokio::test]
    // async fn test_countries_bbox() {
    //     let fs = Arc::new(LocalFileSystem::new_with_prefix(current_dir().unwrap()).unwrap());
    //     let options = FlatGeobufReaderOptions {
    //         bbox: Some((0., -90., 180., 90.)),
    //         ..Default::default()
    //     };
    //     let table =
    //         read_flatgeobuf_async(fs, Path::from("fixtures/flatgeobuf/countries.fgb"), options)
    //             .await
    //             .unwrap();
    //     assert_eq!(table.len(), 133);
    // }

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
