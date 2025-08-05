use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use arrow_schema::{Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::catalog::memory::DataSourceExec;
use datafusion::common::stats::Precision;
use datafusion::common::{GetExt, Statistics};
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::file_format::{FileFormat, FileFormatFactory};
use datafusion::datasource::physical_plan::{FileScanConfig, FileScanConfigBuilder, FileSource};
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::ExecutionPlan;
use geoarrow_flatgeobuf::reader::FlatGeobufHeaderExt;
use geoarrow_flatgeobuf::reader::schema::FlatGeobufSchemaScanner;
use geoarrow_schema::{CoordType, GeoArrowType};
use object_store::path::Path;
use object_store::{ObjectMeta, ObjectStore};

use crate::source::FlatGeobufSource;
use crate::utils::open_flatgeobuf_reader;

/// Factory used to create [`FlatGeobufFormat`]
#[derive(Default, Debug)]
pub struct FlatGeobufFormatFactory {
    coord_type: CoordType,
    prefer_view_types: bool,
    max_read_records: Option<usize>,
}

impl FlatGeobufFormatFactory {
    /// Creates an instance of [`FlatGeobufFormatFactory`]
    pub fn new() -> Self {
        Self {
            coord_type: CoordType::default(),
            prefer_view_types: true,
            max_read_records: Some(1000),
        }
    }
}

impl FileFormatFactory for FlatGeobufFormatFactory {
    fn create(
        &self,
        _state: &dyn Session,
        _format_options: &HashMap<String, String>,
    ) -> Result<Arc<dyn FileFormat>> {
        Ok(Arc::new(FlatGeobufFormat {
            coord_type: self.coord_type,
            prefer_view_types: self.prefer_view_types,
            max_read_records: self.max_read_records,
        }))
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        Arc::new(FlatGeobufFormat {
            coord_type: self.coord_type,
            prefer_view_types: self.prefer_view_types,
            max_read_records: self.max_read_records,
        })
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl GetExt for FlatGeobufFormatFactory {
    fn get_ext(&self) -> String {
        "fgb".to_string()
    }
}

#[derive(Debug)]
pub struct FlatGeobufFormat {
    coord_type: CoordType,
    prefer_view_types: bool,
    max_read_records: Option<usize>,
}

impl Default for FlatGeobufFormat {
    fn default() -> Self {
        Self {
            coord_type: CoordType::default(),
            prefer_view_types: true,
            max_read_records: Some(1000),
        }
    }
}

/// FlatGeobuf allows for **but does not require** schemas to exist in the file metadata.
/// Therefore, if a schema exists in the header, we use that. But if a schema does not exist, we
/// must scan the file to infer the schema.
async fn infer_flatgeobuf_schema(
    store: Arc<dyn ObjectStore>,
    location: Path,
    coord_type: CoordType,
    prefer_view_types: bool,
    max_read_records: Option<usize>,
) -> Result<(SchemaRef, GeoArrowType)> {
    let reader = open_flatgeobuf_reader(store, location).await?;
    let header = reader.header();
    let geometry_type = header
        .geoarrow_type(coord_type)
        .map_err(|err| DataFusionError::External(Box::new(err)))?;
    if let Some(schema) = reader.header().properties_schema(prefer_view_types) {
        Ok((schema, geometry_type))
    } else {
        // Scan to infer schema
        let mut schema_builder = FlatGeobufSchemaScanner::new(prefer_view_types);
        let selection = reader
            .select_all()
            .await
            .map_err(|err| DataFusionError::External(Box::new(err)))?;
        schema_builder
            .process_async(selection, max_read_records)
            .await
            .map_err(|err| DataFusionError::External(Box::new(err)))?;
        Ok((schema_builder.finish(), geometry_type))
    }
}

#[async_trait]
impl FileFormat for FlatGeobufFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_ext(&self) -> String {
        FlatGeobufFormatFactory::new().get_ext()
    }

    fn get_ext_with_compression(
        &self,
        file_compression_type: &FileCompressionType,
    ) -> Result<String> {
        let ext = self.get_ext();
        Ok(format!("{}{}", ext, file_compression_type.get_ext()))
    }

    async fn infer_schema(
        &self,
        _state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> Result<SchemaRef> {
        let mut schemas = vec![];

        for object in objects {
            let (schema, geometry_type) = infer_flatgeobuf_schema(
                store.clone(),
                object.location.clone(),
                self.coord_type,
                self.prefer_view_types,
                self.max_read_records,
            )
            .await?;

            let mut fields = schema.fields().to_vec();
            fields.push(Arc::new(geometry_type.to_field("geometry", true)));
            let schema = Schema::new(fields);

            schemas.push(schema);
        }

        let merged_schema = Schema::try_merge(schemas)?;
        Ok(Arc::new(merged_schema))
    }

    async fn infer_stats(
        &self,
        _state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> Result<Statistics> {
        let reader = open_flatgeobuf_reader(store.clone(), object.location.clone()).await?;

        Ok(
            Statistics::new_unknown(&table_schema).with_num_rows(Precision::Exact(
                reader.header().features_count().try_into().unwrap(),
            )),
        )
    }

    async fn create_physical_plan(
        &self,
        _state: &dyn Session,
        conf: FileScanConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let conf_builder = FileScanConfigBuilder::from(conf);
        let source = Arc::new(FlatGeobufSource::new());
        let config = conf_builder.with_source(source).build();
        Ok(DataSourceExec::from_data_source(config))
    }

    fn file_source(&self) -> Arc<dyn FileSource> {
        Arc::new(FlatGeobufSource::default())
    }

    /// Returns whether this instance uses compression if applicable
    fn compression_type(&self) -> Option<FileCompressionType> {
        Some(FileCompressionType::UNCOMPRESSED)
    }
}

/// Factory for creating FlatGeobuf file formats
#[derive(Default, Debug)]
pub struct FlatGeobufFileFactory {
    file_factory: FlatGeobufFormatFactory,
}

impl FlatGeobufFileFactory {
    pub fn new() -> Self {
        Self {
            file_factory: FlatGeobufFormatFactory::new(),
        }
    }
}

impl FileFormatFactory for FlatGeobufFileFactory {
    fn create(
        &self,
        state: &dyn Session,
        format_options: &std::collections::HashMap<String, String>,
    ) -> Result<Arc<dyn FileFormat>> {
        self.file_factory.create(state, format_options)
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        self.file_factory.default()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl GetExt for FlatGeobufFileFactory {
    fn get_ext(&self) -> String {
        "fgb".to_string()
    }
}
