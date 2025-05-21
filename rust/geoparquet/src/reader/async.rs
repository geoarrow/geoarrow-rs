use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, SchemaRef};
use async_stream::try_stream;
use futures::Stream;
use futures::stream::TryStreamExt;
use geoarrow_schema::error::{GeoArrowError, GeoArrowResult};
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::arrow::async_reader::{
    AsyncFileReader, ParquetRecordBatchStream, ParquetRecordBatchStreamBuilder,
};

use crate::metadata::GeoParquetMetadata;
use crate::reader::builder::GeoParquetReaderBuilder;
use crate::reader::metadata::GeoParquetReaderMetadata;
use crate::reader::options::GeoParquetReaderOptions;
use crate::reader::parse::{infer_target_schema, parse_record_batch};

/// A builder used to construct a [`GeoParquetRecordBatchStream`] for async reading of a GeoParquet
/// file.
///
/// In particular, this handles reading the GeoParquet file metadata, allowing consumers to use
/// this information to select what specific columns, row groups, etc… they wish to be read by the
/// resulting stream
pub struct GeoParquetRecordBatchStreamBuilder<T: AsyncFileReader + Send + 'static> {
    pub(crate) builder: ParquetRecordBatchStreamBuilder<T>,
    geo_meta: Option<GeoParquetMetadata>,
    options: GeoParquetReaderOptions,
}

impl<T: AsyncFileReader + Send + 'static> GeoParquetRecordBatchStreamBuilder<T> {
    /// Construct from a reader
    ///
    /// ```notest
    /// // Open async file
    /// let mut file = tokio::fs::File::new("path.parquet");
    /// // construct the reader
    /// let mut reader = GeoParquetRecordBatchStreamBuilder::new(file).await.unwrap().build().unwrap();
    /// let mut stream = reader.read_stream()
    /// // Read batch
    /// let batch: RecordBatch = reader.next().await.unwrap().unwrap();
    /// ```
    pub async fn try_new(input: T) -> GeoArrowResult<Self> {
        Self::try_new_with_options(input, Default::default(), Default::default()).await
    }

    /// Construct from a reader and options
    pub async fn try_new_with_options(
        mut input: T,
        arrow_options: ArrowReaderOptions,
        geo_options: GeoParquetReaderOptions,
    ) -> GeoArrowResult<Self> {
        let metadata = ArrowReaderMetadata::load_async(&mut input, arrow_options)
            .await
            .map_err(|err| GeoArrowError::External(Box::new(err)))?;
        Ok(Self::new_with_metadata_and_options(
            input,
            metadata,
            geo_options,
        ))
    }

    /// Construct from existing metadata
    pub fn new_with_metadata(input: T, metadata: impl Into<GeoParquetReaderMetadata>) -> Self {
        Self::new_with_metadata_and_options(input, metadata, Default::default())
    }

    /// Construct from existing metadata and options
    pub fn new_with_metadata_and_options(
        input: T,
        metadata: impl Into<GeoParquetReaderMetadata>,
        geo_options: GeoParquetReaderOptions,
    ) -> Self {
        let metadata: GeoParquetReaderMetadata = metadata.into();
        let builder = ParquetRecordBatchStreamBuilder::new_with_metadata(
            input,
            metadata.arrow_metadata().clone(),
        );
        let geo_meta =
            GeoParquetMetadata::from_parquet_meta(builder.metadata().file_metadata()).ok();
        Self {
            builder,
            geo_meta,
            options: geo_options,
        }
    }

    /// Consume this builder, returning a [`GeoParquetRecordBatchStream`]
    pub fn build(self) -> GeoArrowResult<GeoParquetRecordBatchStream<T>> {
        let output_schema = self.output_schema()?;
        let builder = self
            .options
            .apply_to_builder(self.builder, self.geo_meta.as_ref())?;
        let stream = builder
            .build()
            .map_err(|err| GeoArrowError::External(Box::new(err)))?;
        Ok(GeoParquetRecordBatchStream {
            stream,
            output_schema,
        })
    }
}

impl<T: AsyncFileReader + Send + 'static> From<ParquetRecordBatchStreamBuilder<T>>
    for GeoParquetRecordBatchStreamBuilder<T>
{
    fn from(builder: ParquetRecordBatchStreamBuilder<T>) -> Self {
        let geo_meta =
            GeoParquetMetadata::from_parquet_meta(builder.metadata().file_metadata()).ok();
        Self {
            builder,
            geo_meta,
            options: Default::default(),
        }
    }
}

impl<T: AsyncFileReader + Send + 'static> GeoParquetReaderBuilder
    for GeoParquetRecordBatchStreamBuilder<T>
{
    fn output_schema(&self) -> GeoArrowResult<SchemaRef> {
        if let Some(geo_meta) = &self.geo_meta {
            infer_target_schema(self.builder.schema(), geo_meta, self.options.coord_type)
        } else {
            // If non-geospatial, return the same schema as output
            Ok(self.builder.schema().clone())
        }
    }

    fn with_options(self, options: GeoParquetReaderOptions) -> Self {
        Self { options, ..self }
    }
}

/// An interface to a stream that yields [`RecordBatch`] read from a Parquet data source.
///
/// Note that you have to call [`Self::read_stream`] to actually kick off the stream.
///
/// This will parse any geometries to their native representation.
pub struct GeoParquetRecordBatchStream<T: AsyncFileReader + Send + 'static> {
    stream: ParquetRecordBatchStream<T>,
    output_schema: SchemaRef,
}

impl<T: AsyncFileReader + Unpin + Send + 'static> GeoParquetRecordBatchStream<T> {
    /// Start a stream from the file.
    ///
    /// Each Arrow batch will be fetched and any geometry columns will be parsed into the GeoArrow
    /// native representation.
    pub fn read_stream(
        self,
    ) -> impl Stream<Item = std::result::Result<RecordBatch, ArrowError>> + 'static {
        try_stream! {
            for await batch in self.stream {
                yield parse_record_batch(batch?, self.output_schema.clone()).map_err(|err| ArrowError::CastError(err.to_string()))?
            }
        }
    }

    /// Collect all batches into an in-memory table.
    pub async fn read_table(self) -> GeoArrowResult<(Vec<RecordBatch>, SchemaRef)> {
        let output_schema = self.output_schema.clone();
        let batches = self.read_stream().try_collect::<_>().await?;
        Ok((batches, output_schema))
    }
}

#[cfg(all(test, feature = "compression"))]
mod test {
    use tokio::fs::File;

    use super::*;
    use crate::metadata::GeoParquetBboxCovering;
    use crate::test::fixture_dir;

    #[tokio::test]
    async fn nybb() -> GeoArrowResult<()> {
        let fixtures = fixture_dir();
        let file = File::open(fixtures.join("geoparquet/nybb.parquet"))
            .await
            .unwrap();
        let stream = GeoParquetRecordBatchStreamBuilder::try_new(file)
            .await?
            .build()?;
        let _output_geotable = stream.read_table().await?;
        Ok(())
    }

    #[tokio::test]
    async fn overture_buildings() {
        let fixtures = fixture_dir();
        let file = File::open(fixtures.join("geoparquet/overture_buildings.parquet"))
            .await
            .unwrap();
        let reader = GeoParquetRecordBatchStreamBuilder::try_new(file)
            .await
            .unwrap()
            .build()
            .unwrap();
        let (batches, _schema) = reader.read_table().await.unwrap();
        assert_eq!(batches.iter().fold(0, |acc, x| acc + x.num_rows()), 100);
    }

    #[tokio::test]
    async fn overture_buildings_bbox_filter_empty_bbox() {
        let fixtures = fixture_dir();
        let file = File::open(fixtures.join("geoparquet/overture_buildings.parquet"))
            .await
            .unwrap();
        let bbox = geo_types::Rect::new(
            geo_types::coord! { x: -179., y: -55. },
            geo_types::coord! { x: -178., y: -54. },
        );
        let bbox_paths = GeoParquetBboxCovering {
            xmin: vec!["bbox".to_string(), "xmin".to_string()],
            ymin: vec!["bbox".to_string(), "ymin".to_string()],
            zmin: None,
            xmax: vec!["bbox".to_string(), "xmax".to_string()],
            ymax: vec!["bbox".to_string(), "ymax".to_string()],
            zmax: None,
        };
        let reader = GeoParquetRecordBatchStreamBuilder::try_new_with_options(
            file,
            Default::default(),
            GeoParquetReaderOptions::default().with_bbox(bbox, Some(bbox_paths)),
        )
        .await
        .unwrap()
        .build()
        .unwrap();
        let (batches, _schema) = reader.read_table().await.unwrap();
        assert_eq!(batches.iter().fold(0, |acc, x| acc + x.num_rows()), 0);
    }

    #[tokio::test]
    async fn overture_buildings_bbox_filter_full_bbox() {
        let fixtures = fixture_dir();
        let file = File::open(fixtures.join("geoparquet/overture_buildings.parquet"))
            .await
            .unwrap();
        let bbox = geo_types::Rect::new(
            geo_types::coord! { x: 7.393789291381836, y: 50.34489440917969 },
            geo_types::coord! { x: 7.398535251617432, y: 50.34762954711914 },
        );
        let bbox_paths = GeoParquetBboxCovering {
            xmin: vec!["bbox".to_string(), "xmin".to_string()],
            ymin: vec!["bbox".to_string(), "ymin".to_string()],
            zmin: None,
            xmax: vec!["bbox".to_string(), "xmax".to_string()],
            ymax: vec!["bbox".to_string(), "ymax".to_string()],
            zmax: None,
        };
        let reader = GeoParquetRecordBatchStreamBuilder::try_new_with_options(
            file,
            Default::default(),
            GeoParquetReaderOptions::default().with_bbox(bbox, Some(bbox_paths)),
        )
        .await
        .unwrap()
        .build()
        .unwrap();
        let (batches, _schema) = reader.read_table().await.unwrap();
        assert_eq!(batches.iter().fold(0, |acc, x| acc + x.num_rows()), 100);
    }

    #[tokio::test]
    async fn overture_buildings_bbox_filter_partial_bbox() {
        let fixtures = fixture_dir();
        let file = File::open(fixtures.join("geoparquet/overture_buildings.parquet"))
            .await
            .unwrap();
        let bbox = geo_types::Rect::new(
            geo_types::coord! { x: 7.394, y: 50.345 },
            geo_types::coord! { x: 7.398, y: 50.347 },
        );
        let bbox_paths = GeoParquetBboxCovering {
            xmin: vec!["bbox".to_string(), "xmin".to_string()],
            ymin: vec!["bbox".to_string(), "ymin".to_string()],
            zmin: None,
            xmax: vec!["bbox".to_string(), "xmax".to_string()],
            ymax: vec!["bbox".to_string(), "ymax".to_string()],
            zmax: None,
        };
        let reader = GeoParquetRecordBatchStreamBuilder::try_new_with_options(
            file,
            Default::default(),
            GeoParquetReaderOptions::default().with_bbox(bbox, Some(bbox_paths)),
        )
        .await
        .unwrap()
        .build()
        .unwrap();
        let (batches, _schema) = reader.read_table().await.unwrap();
        assert_eq!(batches.iter().fold(0, |acc, x| acc + x.num_rows()), 53);
    }
}
