//! Execution plan for reading FlatGeobuf files

use std::any::Any;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Float64Type;
use arrow_array::{Array, RecordBatch, StructArray};
use arrow_schema::{Schema, SchemaRef};
use datafusion::common::Statistics;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{
    FileMeta, FileOpenFuture, FileOpener, FileScanConfig, FileSource,
};
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use datafusion::physical_plan::ColumnarValue;
use datafusion::physical_plan::filter_pushdown::{FilterPushdownPropagation, PushedDown};
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

    fn with_bbox(self, bbox: [f64; 4]) -> Self {
        Self {
            bbox: Some(bbox),
            ..self
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
        _config: &datafusion::common::config::ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn FileSource>>> {
        let mut pushdown_flags = vec![];
        let mut bbox = self.bbox;
        for filter in filters.iter() {
            if bbox.is_none() {
                if let Some(extracted) = extract_bbox(filter)? {
                    bbox = Some(extracted);
                    pushdown_flags.push(PushedDown::Yes);
                    continue;
                }
            }
            pushdown_flags.push(PushedDown::No);
        }

        if let Some(bbox) = bbox {
            Ok(
                FilterPushdownPropagation::with_parent_pushdown_result(pushdown_flags)
                    .with_updated_node(Arc::new(self.clone().with_bbox(bbox))),
            )
        } else {
            Ok(FilterPushdownPropagation::with_parent_pushdown_result(
                pushdown_flags,
            ))
        }
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

fn columnar_to_bbox(value: ColumnarValue) -> Result<Option<[f64; 4]>> {
    if let ColumnarValue::Array(arr) = value {
        if let Some(struct_arr) = arr.as_any().downcast_ref::<StructArray>() {
            if struct_arr.len() > 0 && struct_arr.num_columns() >= 4 {
                let col0 = struct_arr.column(0).as_primitive_opt::<Float64Type>();
                let col1 = struct_arr.column(1).as_primitive_opt::<Float64Type>();
                let col2 = struct_arr.column(2).as_primitive_opt::<Float64Type>();
                let col3 = struct_arr.column(3).as_primitive_opt::<Float64Type>();
                if let (Some(c0), Some(c1), Some(c2), Some(c3)) = (col0, col1, col2, col3) {
                    let xmin = c0.value(0);
                    let ymin = c1.value(0);
                    let xmax = c2.value(0);
                    let ymax = c3.value(0);
                    return Ok(Some([xmin, ymin, xmax, ymax]));
                }
            }
        }
    }
    Ok(None)
}

fn extract_bbox(expr: &Arc<dyn PhysicalExpr>) -> Result<Option<[f64; 4]>> {
    if let Some(func) = expr.as_any().downcast_ref::<ScalarFunctionExpr>() {
        if func.fun().name().eq_ignore_ascii_case("st_intersects") && func.args().len() == 2 {
            let bbox_expr = func.args()[1].clone();
            let empty = RecordBatch::new_empty(Arc::new(Schema::empty()));
            let value = bbox_expr.evaluate(&empty)?;
            return columnar_to_bbox(value);
        }
    }
    Ok(None)
}
