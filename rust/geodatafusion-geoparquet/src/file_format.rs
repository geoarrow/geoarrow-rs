use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::{GetExt, Statistics};
use datafusion::config::{ConfigField, ConfigFileType, TableParquetOptions};
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::physical_plan::{FileScanConfig, FileSinkConfig, FileSource};
use datafusion::error::Result;
use datafusion::physical_expr::LexRequirement;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_datasource::file_format::FileFormatFactory;
use datafusion_datasource_parquet::ParquetFormat;
use geoarrow_schema::CoordType;
use geoparquet::metadata::GeoParquetMetadata;
use geoparquet::reader::infer_geoarrow_schema;
use object_store::{ObjectMeta, ObjectStore};

use crate::source::GeoParquetSource;

#[derive(Default, Debug)]
pub struct GeoParquetFormatFactory {
    /// inner options for parquet
    pub options: Option<TableParquetOptions>,
}

impl GeoParquetFormatFactory {
    /// Creates an instance of [GeoParquetFormatFactory]
    pub fn new() -> Self {
        Self { options: None }
    }

    /// Creates an instance of [GeoParquetFormatFactory] with customized default options
    pub fn new_with_options(options: TableParquetOptions) -> Self {
        Self {
            options: Some(options),
        }
    }
}

impl FileFormatFactory for GeoParquetFormatFactory {
    fn create(
        &self,
        state: &dyn Session,
        format_options: &HashMap<String, String>,
    ) -> Result<Arc<dyn FileFormat>> {
        let parquet_options = match &self.options {
            None => {
                let mut table_options = state.default_table_options();
                table_options.set_config_format(ConfigFileType::PARQUET);
                table_options.alter_with_string_hash_map(format_options)?;
                table_options.parquet
            }
            Some(parquet_options) => {
                let mut parquet_options = parquet_options.clone();
                for (k, v) in format_options {
                    parquet_options.set(k, v)?;
                }
                parquet_options
            }
        };

        let parquet_format = ParquetFormat::default().with_options(parquet_options);
        Ok(Arc::new(GeoParquetFormat::new(parquet_format)))
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        Arc::new(GeoParquetFormat::default())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl GetExt for GeoParquetFormatFactory {
    fn get_ext(&self) -> String {
        "parquet".to_string()
    }
}

/// GeoParquet `FileFormat` implementation
#[derive(Debug, Default)]
pub struct GeoParquetFormat {
    inner: ParquetFormat,
    parse_to_native: bool,
    coord_type: CoordType,
}

impl GeoParquetFormat {
    /// Creates a new instance of `GeoParquetFormat`
    pub fn new(format: ParquetFormat) -> Self {
        Self {
            inner: format.with_skip_metadata(false),
            parse_to_native: false,
            coord_type: CoordType::default(),
        }
    }
}

#[async_trait]
impl FileFormat for GeoParquetFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_ext(&self) -> String {
        self.inner.get_ext()
    }

    fn get_ext_with_compression(
        &self,
        file_compression_type: &FileCompressionType,
    ) -> Result<String> {
        self.inner.get_ext_with_compression(file_compression_type)
    }

    fn compression_type(&self) -> Option<FileCompressionType> {
        self.inner.compression_type()
    }

    async fn infer_schema(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> Result<SchemaRef> {
        let schema = self.inner.infer_schema(state, store, objects).await?;
        // Insert GeoArrow metadata onto geometry column
        if let Some(geo_meta_str) = schema.metadata().get("geo") {
            let geo_meta: GeoParquetMetadata = serde_json::from_str(geo_meta_str).unwrap();
            let new_schema =
                infer_geoarrow_schema(&schema, &geo_meta, self.parse_to_native, self.coord_type)
                    .unwrap();
            Ok(new_schema)
        } else {
            Ok(schema)
        }
    }

    async fn infer_stats(
        &self,
        _state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> Result<Statistics> {
        self.inner
            .infer_stats(_state, store, table_schema, object)
            .await
    }

    async fn create_physical_plan(
        &self,
        _state: &dyn Session,
        conf: FileScanConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.inner.create_physical_plan(_state, conf).await
    }

    async fn create_writer_physical_plan(
        &self,
        _input: Arc<dyn ExecutionPlan>,
        _state: &dyn Session,
        _conf: FileSinkConfig,
        _order_requirements: Option<LexRequirement>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        todo!("writing not implemented for GeoParquet yet")
    }

    fn file_source(&self) -> Arc<dyn FileSource> {
        Arc::new(GeoParquetSource::default())
    }
}
