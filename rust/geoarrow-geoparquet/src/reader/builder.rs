use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, SchemaRef};
use geoarrow_array::error::{GeoArrowError, Result};
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReader,
    ParquetRecordBatchReaderBuilder,
};
use parquet::file::reader::ChunkReader;

use crate::metadata::GeoParquetMetadata;
use crate::reader::metadata::GeoParquetReaderMetadata;
use crate::reader::options::GeoParquetReaderOptions;
use crate::reader::parse::{infer_target_schema, parse_record_batch};

pub trait GeoParquetReaderBuilder: Sized {
    fn output_schema(&self) -> Result<SchemaRef>;

    fn with_options(self, options: GeoParquetReaderOptions) -> Self;
}

/// A synchronous builder used to construct [`GeoParquetRecordBatchReader`] for a file.
///
/// For an async API see [`crate::io::parquet::GeoParquetRecordBatchStreamBuilder`]
pub struct GeoParquetRecordBatchReaderBuilder<T: ChunkReader + 'static> {
    builder: ParquetRecordBatchReaderBuilder<T>,
    geo_meta: Option<GeoParquetMetadata>,
    options: GeoParquetReaderOptions,
}

impl<T: ChunkReader + 'static> GeoParquetRecordBatchReaderBuilder<T> {
    /// Construct from a reader
    ///
    /// ```notest
    /// use std::fs::File;
    ///
    /// let file = File::open("fixtures/geoparquet/nybb.parquet").unwrap();
    /// let reader = GeoParquetRecordBatchReaderBuilder::try_new(file).unwrap().build().unwrap();
    /// // Read batch
    /// let batch: RecordBatch = reader.next().unwrap().unwrap();
    /// ```
    pub fn try_new(reader: T) -> Result<Self> {
        Self::try_new_with_options(reader, Default::default(), Default::default())
    }

    /// Construct from a reader and options
    pub fn try_new_with_options(
        reader: T,
        arrow_options: ArrowReaderOptions,
        geo_options: GeoParquetReaderOptions,
    ) -> Result<Self> {
        let metadata = ArrowReaderMetadata::load(&reader, arrow_options)
            .map_err(|err| GeoArrowError::External(Box::new(err)))?;
        Ok(Self::new_with_metadata_and_options(
            reader,
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
        let builder = ParquetRecordBatchReaderBuilder::new_with_metadata(
            input,
            metadata.arrow_metadata().clone(),
        );
        Self::from(builder).with_options(geo_options)
    }

    /// Returns a reference to the geo metadata.
    pub fn geo_meta(&self) -> Option<&GeoParquetMetadata> {
        self.geo_meta.as_ref()
    }

    /// Consume this builder, returning a [`GeoParquetRecordBatchReader`]
    pub fn build(self) -> Result<GeoParquetRecordBatchReader> {
        let output_schema = self.output_schema()?;
        let builder = self
            .options
            .apply_to_builder(self.builder, self.geo_meta.as_ref())?;
        let reader = builder
            .build()
            .map_err(|err| GeoArrowError::External(Box::new(err)))?;
        Ok(GeoParquetRecordBatchReader {
            reader,
            output_schema,
        })
    }
}

impl<T: ChunkReader + 'static> From<ParquetRecordBatchReaderBuilder<T>>
    for GeoParquetRecordBatchReaderBuilder<T>
{
    fn from(builder: ParquetRecordBatchReaderBuilder<T>) -> Self {
        let geo_meta =
            GeoParquetMetadata::from_parquet_meta(builder.metadata().file_metadata()).ok();
        Self {
            builder,
            geo_meta,
            options: Default::default(),
        }
    }
}

impl<T: ChunkReader + 'static> GeoParquetReaderBuilder for GeoParquetRecordBatchReaderBuilder<T> {
    fn output_schema(&self) -> Result<SchemaRef> {
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

/// An `Iterator<Item = ArrowResult<RecordBatch>>` that yields [`RecordBatch`]
/// read from a Parquet data source.
/// This will parse any geometries to their native representation.
pub struct GeoParquetRecordBatchReader {
    reader: ParquetRecordBatchReader,
    output_schema: SchemaRef,
}

impl GeoParquetRecordBatchReader {
    // pub fn read_table(self) -> Result<Table> {
    //     let output_schema = self.output_schema.clone();
    //     let batches = self.collect::<std::result::Result<Vec<_>, ArrowError>>()?;
    //     Table::try_new(batches, output_schema)
    // }
}

impl Iterator for GeoParquetRecordBatchReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(batch) = self.reader.next() {
            match batch {
                Ok(batch) => Some(
                    parse_record_batch(batch, self.output_schema.clone())
                        .map_err(|err| ArrowError::CastError(err.to_string())),
                ),
                Err(err) => Some(Err(err)),
            }
        } else {
            None
        }
    }
}

impl RecordBatchReader for GeoParquetRecordBatchReader {
    fn schema(&self) -> arrow_schema::SchemaRef {
        self.output_schema.clone()
    }
}

#[cfg(all(test, feature = "compression"))]
mod test {
    use arrow_array::cast::AsArray;

    use super::*;
    use std::fs::File;

    use crate::metadata::GeoParquetBboxCovering;
    use crate::test::fixture_dir;

    #[test]
    fn nybb() {
        let fixtures = fixture_dir();

        let file = File::open(fixtures.join("geoparquet/nybb.parquet")).unwrap();
        let reader = GeoParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let _batches = reader
            .collect::<std::result::Result<Vec<_>, ArrowError>>()
            .unwrap();
    }

    #[test]
    fn nybb_geoarrow() {
        let fixtures = fixture_dir();
        let file = File::open(fixtures.join("geoparquet/nybb_geoarrow.parquet")).unwrap();
        let reader = GeoParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let _batches = reader
            .collect::<std::result::Result<Vec<_>, ArrowError>>()
            .unwrap();
    }

    #[test]
    fn nybb_geoarrow_bbox_filter() {
        let fixtures = fixture_dir();
        let file = File::open(fixtures.join("geoparquet/nybb_geoarrow.parquet")).unwrap();
        // projected bounds of a part of Staten Island
        let bbox = [
            930504.8649454953,
            136494.45816818904,
            952421.2755293427,
            162625.81161641632,
        ];
        let bbox = geo_types::Rect::new(
            geo_types::coord! { x: bbox[0], y: bbox[1] },
            geo_types::coord! { x: bbox[2], y: bbox[3] },
        );
        let reader = GeoParquetRecordBatchReaderBuilder::try_new_with_options(
            file,
            Default::default(),
            GeoParquetReaderOptions::default().with_bbox(bbox, None),
        )
        .unwrap()
        .build()
        .unwrap();

        let batches = reader
            .collect::<std::result::Result<Vec<_>, ArrowError>>()
            .unwrap();
        let value = batches[0]
            .column_by_name("BoroName")
            .unwrap()
            .as_string::<i32>()
            .value(0);
        assert_eq!(value, "Staten Island");
    }

    #[test]
    fn overture_buildings() {
        let fixtures = fixture_dir();
        let file = File::open(fixtures.join("geoparquet/overture_buildings.parquet")).unwrap();
        let reader = GeoParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let batches = reader
            .collect::<std::result::Result<Vec<_>, ArrowError>>()
            .unwrap();
        assert_eq!(batches.iter().fold(0, |acc, x| acc + x.num_rows()), 100);
    }

    #[test]
    fn overture_buildings_bbox_filter_empty_bbox() {
        let fixtures = fixture_dir();
        let file = File::open(fixtures.join("geoparquet/overture_buildings.parquet")).unwrap();
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
        let reader = GeoParquetRecordBatchReaderBuilder::try_new_with_options(
            file,
            Default::default(),
            GeoParquetReaderOptions::default().with_bbox(bbox, Some(bbox_paths)),
        )
        .unwrap()
        .build()
        .unwrap();
        let batches = reader
            .collect::<std::result::Result<Vec<_>, ArrowError>>()
            .unwrap();
        assert_eq!(batches.iter().fold(0, |acc, x| acc + x.num_rows()), 0);
    }

    #[test]
    fn overture_buildings_bbox_filter_full_bbox() {
        let fixtures = fixture_dir();
        let file = File::open(fixtures.join("geoparquet/overture_buildings.parquet")).unwrap();
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
        let reader = GeoParquetRecordBatchReaderBuilder::try_new_with_options(
            file,
            Default::default(),
            GeoParquetReaderOptions::default().with_bbox(bbox, Some(bbox_paths)),
        )
        .unwrap()
        .build()
        .unwrap();
        let batches = reader
            .collect::<std::result::Result<Vec<_>, ArrowError>>()
            .unwrap();
        assert_eq!(batches.iter().fold(0, |acc, x| acc + x.num_rows()), 100);
    }

    #[test]
    fn overture_buildings_bbox_filter_partial_bbox() {
        let fixtures = fixture_dir();
        let file = File::open(fixtures.join("geoparquet/overture_buildings.parquet")).unwrap();
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
        let reader = GeoParquetRecordBatchReaderBuilder::try_new_with_options(
            file,
            Default::default(),
            GeoParquetReaderOptions::default().with_bbox(bbox, Some(bbox_paths)),
        )
        .unwrap()
        .build()
        .unwrap();
        let batches = reader
            .collect::<std::result::Result<Vec<_>, ArrowError>>()
            .unwrap();
        assert_eq!(batches.iter().fold(0, |acc, x| acc + x.num_rows()), 53);
    }
}
