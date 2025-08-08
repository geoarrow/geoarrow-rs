use std::any::Any;
use std::sync::Arc;

use arrow_array::RecordBatchIterator;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use bytes::Bytes;
use datafusion::common::runtime::SpawnedTask;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::display::{DisplayAs, DisplayFormatType};
use datafusion_datasource::display::FileGroupDisplay;
use datafusion_datasource::file_sink_config::{FileSink, FileSinkConfig};
use datafusion_datasource::sink::DataSink;
use datafusion_datasource::write::demux::DemuxedStreamReceiver;
use datafusion_datasource::write::get_writer_schema;
use geoarrow_array::geozero::export::GeozeroRecordBatchReader;
use geoarrow_flatgeobuf::writer::{write_flatgeobuf, FlatGeobufWriterOptions};
use object_store::ObjectStore;

#[derive(Debug)]
pub struct FlatGeobufSink {
    config: FileSinkConfig,
}

impl FlatGeobufSink {
    pub fn new(config: FileSinkConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl FileSink for FlatGeobufSink {
    fn config(&self) -> &FileSinkConfig {
        &self.config
    }

    async fn spawn_writer_tasks_and_join(
        &self,
        _context: &Arc<TaskContext>,
        demux_task: SpawnedTask<Result<()>>,
        mut file_stream_rx: DemuxedStreamReceiver,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<u64> {
        let mut rows_written = 0u64;

        while let Some((path, mut rx)) = file_stream_rx.recv().await {
            let mut batches = Vec::new();
            while let Some(batch) = rx.recv().await {
                rows_written += batch.num_rows() as u64;
                batches.push(batch);
            }

            let schema = get_writer_schema(&self.config);
            let reader = GeozeroRecordBatchReader::new(Box::new(RecordBatchIterator::new(
                batches.into_iter().map(Ok),
                schema.clone(),
            )));

            let name = path
                .filename()
                .map(|s| s.to_string())
                .unwrap_or_else(|| "data".to_string());
            let mut buffer = Vec::new();
            write_flatgeobuf(reader, &mut buffer, FlatGeobufWriterOptions::new(name))
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            object_store
                .put(&path, Bytes::from(buffer).into())
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
        }

        demux_task
            .join_unwind()
            .await
            .map_err(|e| DataFusionError::ExecutionJoin(Box::new(e)))??;

        Ok(rows_written)
    }
}

#[async_trait]
impl DataSink for FlatGeobufSink {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> &SchemaRef {
        self.config.output_schema()
    }

    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
    ) -> Result<u64> {
        FileSink::write_all(self, data, context).await
    }
}

impl DisplayAs for FlatGeobufSink {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "FlatGeobufSink(file_groups=")?;
                FileGroupDisplay(&self.config.file_group).fmt_as(t, f)?;
                write!(f, ")")
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "format: flatgeobuf")?;
                write!(f, "file={}", &self.config.original_url)
            }
        }
    }
}
