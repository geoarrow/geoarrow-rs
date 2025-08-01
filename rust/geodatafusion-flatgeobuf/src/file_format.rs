use std::any::Any;
use std::collections::{HashMap, HashSet};
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
use flatgeobuf::HttpFgbReader;
use geoarrow_flatgeobuf::reader::object_store::ObjectStoreWrapper;
use geoarrow_flatgeobuf::reader::{HeaderInfo, parse_header};
use geoarrow_schema::CoordType;
use http_range_client::AsyncBufferedHttpRangeClient;
use object_store::path::Path;
use object_store::{ObjectMeta, ObjectStore};

use crate::source::FlatGeobufSource;

/// Factory used to create [`FlatGeobufFormat`]
#[derive(Default, Debug)]
pub struct FlatGeobufFormatFactory {
    coord_type: CoordType,
}

impl FlatGeobufFormatFactory {
    /// Creates an instance of [`FlatGeobufFormatFactory`]
    pub fn new() -> Self {
        Self {
            coord_type: CoordType::default(),
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
        }))
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        Arc::new(FlatGeobufFormat {
            coord_type: self.coord_type,
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

#[derive(Debug, Default)]
pub struct FlatGeobufFormat {
    coord_type: CoordType,
}

async fn read_flatgeobuf_header(
    store: Arc<dyn ObjectStore>,
    location: Path,
    coord_type: CoordType,
    prefer_view_types: bool,
    projection: Option<&HashSet<String>>,
) -> Result<HeaderInfo> {
    let object_store_wrapper = ObjectStoreWrapper::new(store, location);
    let async_client = AsyncBufferedHttpRangeClient::with(object_store_wrapper, "");
    let reader = HttpFgbReader::new(async_client)
        .await
        .map_err(|err| DataFusionError::External(Box::new(err)))?;
    let header = parse_header(reader.header(), coord_type, prefer_view_types, projection)
        .map_err(|err| DataFusionError::External(Box::new(err)))?;
    Ok(header)
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
            let header = read_flatgeobuf_header(
                store.clone(),
                object.location.clone(),
                self.coord_type,
                true,
                None,
            )
            .await?;

            let mut fields = header
                .properties_schema()
                .expect("todo: handle inferring schema")
                .fields()
                .to_vec();
            fields.push(Arc::new(header.geometry_type().to_field("geometry", true)));
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
        let header = read_flatgeobuf_header(
            store.clone(),
            object.location.clone(),
            self.coord_type,
            true,
            None,
        )
        .await?;

        Ok(
            Statistics::new_unknown(&table_schema).with_num_rows(Precision::Exact(
                header.features_count().try_into().unwrap(),
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
        dbg!(&config);
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
