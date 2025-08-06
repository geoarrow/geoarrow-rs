//! Execution plan for reading FlatGeobuf files

use std::any::Any;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use datafusion::common::Statistics;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{
    FileMeta, FileOpenFuture, FileOpener, FileScanConfig, FileSource,
};
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use futures::StreamExt;
use geoarrow_flatgeobuf::reader::{FlatGeobufReaderOptions, FlatGeobufRecordBatchStream};
use object_store::ObjectStore;

use crate::utils::open_flatgeobuf_reader;

#[derive(Debug, Clone, Default)]
pub struct FlatGeobufSource {
    batch_size: Option<usize>,
    file_schema: Option<SchemaRef>,
    projection: Option<Vec<usize>>,
    metrics: ExecutionPlanMetricsSet,
    projected_statistics: Option<Statistics>,
}

impl FlatGeobufSource {
    pub fn new() -> Self {
        Self {
            batch_size: None,
            file_schema: None,
            projection: None,
            metrics: ExecutionPlanMetricsSet::new(),
            projected_statistics: None,
        }
    }
}

impl From<FlatGeobufSource> for Arc<dyn FileSource> {
    fn from(source: FlatGeobufSource) -> Self {
        Arc::new(source)
    }
}

impl FileSource for FlatGeobufSource {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        _base_config: &FileScanConfig,
        _partition: usize,
    ) -> Arc<dyn FileOpener> {
        Arc::new(FlatGeobufOpener::new(Arc::new(self.clone()), object_store))
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        let mut conf = self.clone();
        conf.batch_size = Some(batch_size);
        Arc::new(conf)
    }

    fn with_schema(&self, schema: SchemaRef) -> Arc<dyn FileSource> {
        let mut conf = self.clone();
        conf.file_schema = Some(schema);
        Arc::new(conf)
    }

    fn with_statistics(&self, statistics: Statistics) -> Arc<dyn FileSource> {
        let mut conf = self.clone();
        conf.projected_statistics = Some(statistics);
        Arc::new(conf)
    }

    fn with_projection(&self, config: &FileScanConfig) -> Arc<dyn FileSource> {
        let mut conf = self.clone();
        conf.projection = config.projection.clone();
        Arc::new(conf)
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    fn statistics(&self) -> Result<Statistics> {
        let statistics = &self.projected_statistics;
        Ok(statistics
            .clone()
            .expect("projected_statistics must be set"))
    }

    fn file_type(&self) -> &str {
        "flatgeobuf"
    }
}

pub struct FlatGeobufOpener {
    config: Arc<FlatGeobufSource>,
    object_store: Arc<dyn ObjectStore>,
}

impl FlatGeobufOpener {
    pub fn new(config: Arc<FlatGeobufSource>, object_store: Arc<dyn ObjectStore>) -> Self {
        Self {
            config,
            object_store,
        }
    }
}

impl FileOpener for FlatGeobufOpener {
    fn open(&self, file_meta: FileMeta, _file: PartitionedFile) -> Result<FileOpenFuture> {
        let store = Arc::clone(&self.object_store);
        let config = self.config.clone();

        Ok(Box::pin(async move {
            let mut file_schema = config
                .file_schema
                .clone()
                .expect("Expected file schema to be set");
            if let Some(projection) = config.projection.clone() {
                file_schema = Arc::new(file_schema.project(&projection)?);
            }

            let options = FlatGeobufReaderOptions::from_combined_schema(file_schema)
                .map_err(|err| DataFusionError::External(Box::new(err)))?
                .with_batch_size(config.batch_size.unwrap_or(1024));

            let fgb_reader = open_flatgeobuf_reader(store, file_meta.location().clone()).await?;
            let selection = fgb_reader
                .select_all()
                .await
                .map_err(|err| DataFusionError::External(Box::new(err)))?;
            let stream = FlatGeobufRecordBatchStream::try_new(selection, options)
                .map_err(|err| DataFusionError::External(Box::new(err)))?;
            Ok(stream.boxed())
        }))
    }
}
