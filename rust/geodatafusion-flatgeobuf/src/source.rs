//! Execution plan for reading FlatGeobuf files

use std::any::Any;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use datafusion::common::Statistics;
use datafusion::config::ConfigOptions;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{
    FileMeta, FileOpenFuture, FileOpener, FileScanConfig, FileSource,
};
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_expr::expressions::Literal;
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use datafusion::physical_plan::filter_pushdown::{FilterPushdownPropagation, PushedDown};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::scalar::ScalarValue;
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
    bbox: Option<[f64; 4]>,
}

impl FlatGeobufSource {
    pub fn new() -> Self {
        Self {
            batch_size: None,
            file_schema: None,
            projection: None,
            metrics: ExecutionPlanMetricsSet::new(),
            projected_statistics: None,
            bbox: None,
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

    fn try_pushdown_filters(
        &self,
        filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn FileSource>>> {
        let bbox = filters.iter().find_map(extract_bbox);

        let mut result: FilterPushdownPropagation<Arc<dyn FileSource>> =
            FilterPushdownPropagation::with_parent_pushdown_result(vec![
                PushedDown::No;
                filters.len()
            ]);

        if let Some(bbox) = bbox {
            let mut conf = self.clone();
            conf.bbox = Some(bbox);
            result = result.with_updated_node(Arc::new(conf) as Arc<dyn FileSource>);
        }

        Ok(result)
    }
}

fn extract_bbox(expr: &Arc<dyn PhysicalExpr>) -> Option<[f64; 4]> {
    let func = expr.as_any().downcast_ref::<ScalarFunctionExpr>()?;
    if !func.name().eq_ignore_ascii_case("st_intersects") || func.args().len() != 2 {
        return None;
    }
    let geom_expr = &func.args()[1];
    let env_func = geom_expr.as_any().downcast_ref::<ScalarFunctionExpr>()?;
    if !env_func.name().eq_ignore_ascii_case("st_makeenvelope") || env_func.args().len() < 4 {
        return None;
    }
    let x_min = value_as_f64(&env_func.args()[0])?;
    let y_min = value_as_f64(&env_func.args()[1])?;
    let x_max = value_as_f64(&env_func.args()[2])?;
    let y_max = value_as_f64(&env_func.args()[3])?;
    Some([x_min, y_min, x_max, y_max])
}

fn value_as_f64(expr: &Arc<dyn PhysicalExpr>) -> Option<f64> {
    let lit = expr.as_any().downcast_ref::<Literal>()?;
    match lit.value() {
        ScalarValue::Float64(Some(v)) => Some(*v),
        ScalarValue::Float32(Some(v)) => Some(*v as f64),
        ScalarValue::Int64(Some(v)) => Some(*v as f64),
        ScalarValue::Int32(Some(v)) => Some(*v as f64),
        _ => None,
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
            let selection = if let Some([minx, miny, maxx, maxy]) = config.bbox {
                fgb_reader
                    .select_bbox(minx, miny, maxx, maxy)
                    .await
                    .map_err(|err| DataFusionError::External(Box::new(err)))?
            } else {
                fgb_reader
                    .select_all()
                    .await
                    .map_err(|err| DataFusionError::External(Box::new(err)))?
            };
            let stream = FlatGeobufRecordBatchStream::try_new(selection, options)
                .map_err(|err| DataFusionError::External(Box::new(err)))?;
            Ok(stream.boxed())
        }))
    }
}
