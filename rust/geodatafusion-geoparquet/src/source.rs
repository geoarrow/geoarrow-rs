use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use datafusion::common::Statistics;
use datafusion::config::ConfigOptions;
use datafusion::datasource::physical_plan::{FileScanConfig, FileSource};
use datafusion::datasource::schema_adapter::SchemaAdapterFactory;
use datafusion::error::Result;
use datafusion::physical_plan::filter_pushdown::FilterPushdownPropagation;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::{DisplayFormatType, PhysicalExpr};
use datafusion_datasource_parquet::source::ParquetSource;

#[derive(Clone, Default, Debug)]
pub struct GeoParquetSource {
    inner: ParquetSource,
}

/// Allows easy conversion from ParquetSource to Arc\<dyn FileSource\>;
impl From<GeoParquetSource> for Arc<dyn FileSource> {
    fn from(source: GeoParquetSource) -> Self {
        Arc::new(source)
    }
}

impl FileSource for GeoParquetSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn object_store::ObjectStore>,
        base_config: &datafusion::datasource::physical_plan::FileScanConfig,
        partition: usize,
    ) -> Arc<dyn datafusion::datasource::physical_plan::FileOpener> {
        self.inner
            .create_file_opener(object_store, base_config, partition)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        self.inner.with_batch_size(batch_size)
    }

    fn with_schema(&self, schema: SchemaRef) -> Arc<dyn FileSource> {
        self.inner.with_schema(schema)
    }

    fn with_statistics(&self, statistics: Statistics) -> Arc<dyn FileSource> {
        self.inner.with_statistics(statistics)
    }

    fn with_projection(&self, config: &FileScanConfig) -> Arc<dyn FileSource> {
        self.inner.with_projection(config)
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        self.inner.metrics()
    }

    fn statistics(&self) -> Result<Statistics> {
        self.inner.statistics()
    }

    fn file_type(&self) -> &str {
        self.inner.file_type()
    }

    fn fmt_extra(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        self.inner.fmt_extra(t, f)
    }

    fn try_pushdown_filters(
        &self,
        filters: Vec<Arc<dyn PhysicalExpr>>,
        config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn FileSource>>> {
        self.inner.try_pushdown_filters(filters, config)
    }

    fn with_schema_adapter_factory(
        &self,
        schema_adapter_factory: Arc<dyn SchemaAdapterFactory>,
    ) -> Result<Arc<dyn FileSource>> {
        self.inner
            .with_schema_adapter_factory(schema_adapter_factory)
    }

    fn schema_adapter_factory(&self) -> Option<Arc<dyn SchemaAdapterFactory>> {
        self.inner.schema_adapter_factory()
    }
}
