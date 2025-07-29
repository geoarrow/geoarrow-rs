use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use arrow_schema::{Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::catalog::memory::DataSourceExec;
use datafusion::common::{GetExt, Statistics};
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::file_format::{FileFormat, FileFormatFactory};
use datafusion::datasource::physical_plan::{
    FileScanConfig, FileScanConfigBuilder, FileSinkConfig, FileSource,
};
use datafusion::error::Result;
use datafusion::physical_expr::LexRequirement;
use datafusion::physical_plan::ExecutionPlan;
use geoarrow_flatgeobuf::reader::FlatGeobufStreamBuilder;
use geoarrow_schema::CoordType;
use object_store::{ObjectMeta, ObjectStore};

use crate::source::FlatGeobufSource;

#[derive(Default, Debug)]
/// Factory used to create [`FlatGeobufFormat`]
pub struct FlatGeobufFormatFactory {
    // the options for FlatGeobuf file read
    // pub options: Option<CsvOptions>,
}

impl FlatGeobufFormatFactory {
    /// Creates an instance of [`FlatGeobufFormatFactory`]
    pub fn new() -> Self {
        Self {}
    }
}

impl FileFormatFactory for FlatGeobufFormatFactory {
    fn create(
        &self,
        _state: &dyn Session,
        _format_options: &HashMap<String, String>,
    ) -> Result<Arc<dyn FileFormat>> {
        Ok(Arc::new(FlatGeobufFormat::default()))

        // let csv_options = match &self.options {
        //     None => {
        //         let mut table_options = state.default_table_options();
        //         table_options.set_config_format(ConfigFileType::CSV);
        //         table_options.alter_with_string_hash_map(format_options)?;
        //         table_options.csv
        //     }
        //     Some(csv_options) => {
        //         let mut csv_options = csv_options.clone();
        //         for (k, v) in format_options {
        //             csv_options.set(k, v)?;
        //         }
        //         csv_options
        //     }
        // };

        // Ok(Arc::new(CsvFormat::default().with_options(csv_options)))
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        Arc::new(FlatGeobufFormat::default())
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

#[derive(Debug, Default)]
pub struct FlatGeobufFormat {}

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
            let builder =
                FlatGeobufStreamBuilder::new_from_store(store.clone(), object.location.clone())
                    .await
                    .unwrap();
            let schema = builder
                .output_schema(CoordType::Separated, true, None)
                .unwrap();
            schemas.push(schema);
        }

        let merged_schema = Schema::try_merge(schemas)?;
        Ok(Arc::new(merged_schema))
    }

    async fn infer_stats(
        &self,
        _state: &dyn Session,
        _store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        _object: &ObjectMeta,
    ) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&table_schema))
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

    async fn create_writer_physical_plan(
        &self,
        _input: Arc<dyn ExecutionPlan>,
        _state: &dyn Session,
        _conf: FileSinkConfig,
        _order_requirements: Option<LexRequirement>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        todo!()
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
