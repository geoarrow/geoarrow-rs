use std::any::Any;
use std::collections::HashMap;
use std::fmt::{self, Debug, Formatter};
use std::io::{BufWriter, Write};
use std::sync::Arc;

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::runtime::SpawnedTask;
use datafusion::common::{GetExt, Statistics};
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::file_format::{FileFormat, FileFormatFactory};
use datafusion::datasource::physical_plan::{FileScanConfig, FileSource};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::LexRequirement;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan};
use datafusion_datasource::display::FileGroupDisplay;
use datafusion_datasource::file_sink_config::{FileSink, FileSinkConfig};
use datafusion_datasource::sink::{DataSink, DataSinkExec};
use datafusion_datasource::write::ObjectWriterBuilder;
use datafusion_datasource::write::demux::DemuxedStreamReceiver;
use geoarrow_geojson::writer::GeoJsonLinesWriter;
use object_store::{ObjectMeta, ObjectStore};
use tempfile::NamedTempFile;
use tokio::io::AsyncWriteExt;

/// Factory used to create [`GeoJsonFormat`]
///
/// This always outputs newline-delimited GeoJSON.
#[derive(Debug, Default)]
pub struct GeoJsonFormatFactory {}

impl GeoJsonFormatFactory {
    /// Creates an instance of [`GeoJsonFormatFactory`]
    pub fn new() -> Self {
        Self {}
    }
}

impl FileFormatFactory for GeoJsonFormatFactory {
    fn create(
        &self,
        _state: &dyn Session,
        _format_options: &HashMap<String, String>,
    ) -> Result<Arc<dyn FileFormat>> {
        Ok(Arc::new(GeoJsonFormat {}))
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        Arc::new(GeoJsonFormat {})
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl GetExt for GeoJsonFormatFactory {
    fn get_ext(&self) -> String {
        "geojsonl".to_string()
    }
}

#[derive(Debug, Default)]
pub struct GeoJsonFormat {}

#[async_trait]
impl FileFormat for GeoJsonFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_ext(&self) -> String {
        "geojsonl".to_string()
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
        _store: &Arc<dyn ObjectStore>,
        _objects: &[ObjectMeta],
    ) -> Result<SchemaRef> {
        // GeoJSON files cannot be read by this implementation, only written
        Err(DataFusionError::NotImplemented(
            "Reading GeoJSON files is not currently supported. This format only supports writing."
                .to_string(),
        ))
    }

    async fn infer_stats(
        &self,
        _state: &dyn Session,
        _store: &Arc<dyn ObjectStore>,
        _table_schema: SchemaRef,
        _object: &ObjectMeta,
    ) -> Result<Statistics> {
        Err(DataFusionError::NotImplemented(
            "Reading GeoJSON files is not currently supported. This format only supports writing."
                .to_string(),
        ))
    }

    async fn create_physical_plan(
        &self,
        _state: &dyn Session,
        _conf: FileScanConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::NotImplemented(
            "Reading GeoJSON files is not currently supported. This format only supports writing."
                .to_string(),
        ))
    }

    async fn create_writer_physical_plan(
        &self,
        input: Arc<dyn ExecutionPlan>,
        _state: &dyn Session,
        conf: FileSinkConfig,
        order_requirements: Option<LexRequirement>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let sink = Arc::new(GeoJsonSink::new(conf));
        Ok(Arc::new(DataSinkExec::new(input, sink, order_requirements)))
    }

    fn file_source(&self) -> Arc<dyn FileSource> {
        // Not implemented for reading
        unimplemented!("GeoJSON format only supports writing")
    }

    /// Returns whether this instance uses compression if applicable
    fn compression_type(&self) -> Option<FileCompressionType> {
        Some(FileCompressionType::UNCOMPRESSED)
    }
}

#[derive(Debug)]
pub struct GeoJsonSink {
    config: FileSinkConfig,
}

impl GeoJsonSink {
    pub fn new(config: FileSinkConfig) -> Self {
        Self { config }
    }
}

impl DisplayAs for GeoJsonSink {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "GeoJsonSink(file_groups=")?;
                FileGroupDisplay(&self.config.file_group).fmt_as(t, f)?;
                write!(f, ")")
            }
            DisplayFormatType::TreeRender => {
                let format_name = "geojson-lines";
                writeln!(f, "format: {}", format_name)?;
                write!(f, "file={}", &self.config.original_url)
            }
        }
    }
}

#[async_trait]
impl FileSink for GeoJsonSink {
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
        let mut total_rows: u64 = 0;
        while let Some((path, mut rb_rx)) = file_stream_rx.recv().await {
            // We create a tempfile on disk because the GeoJSON writers are sync only
            let named_temp_file = NamedTempFile::new()?;
            let output_file = BufWriter::new(named_temp_file);

            let mut geojson_writer = GeoJsonLinesWriter::new(output_file);

            // For each record batch received, write it to the GeoJSON writer
            while let Some(batch) = rb_rx.recv().await {
                total_rows += batch.num_rows() as u64;
                geojson_writer
                    .write(&batch)
                    .map_err(|err| DataFusionError::External(Box::new(err)))?;
            }

            // Finalize the writer
            let mut output_file = geojson_writer
                .finish()
                .map_err(|err| DataFusionError::External(Box::new(err)))?;
            output_file.flush()?;

            let named_temp_file = output_file
                .into_inner()
                .map_err(|err| DataFusionError::External(Box::new(err)))?;

            // Upload temp file to object store
            upload_temp_file_to_object_store(named_temp_file, &path, object_store.clone()).await?;
        }

        demux_task
            .join()
            .await
            .map_err(|e| DataFusionError::Execution(e.to_string()))??;
        Ok(total_rows)
    }
}

#[async_trait]
impl DataSink for GeoJsonSink {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> &SchemaRef {
        self.config.output_schema()
    }

    async fn write_all(
        &self,
        data: datafusion::execution::SendableRecordBatchStream,
        context: &Arc<TaskContext>,
    ) -> Result<u64> {
        FileSink::write_all(self, data, context).await
    }
}

/// Helper function to upload a temp file to object store
async fn upload_temp_file_to_object_store(
    named_temp_file: tempfile::NamedTempFile,
    path: &object_store::path::Path,
    object_store: Arc<dyn ObjectStore>,
) -> Result<()> {
    use std::io::Read;

    // Reopen the temp file for reading
    let mut buf_reader = std::io::BufReader::new(std::fs::File::open(named_temp_file.path())?);

    let mut object_writer = ObjectWriterBuilder::new(
        FileCompressionType::UNCOMPRESSED,
        path,
        object_store.clone(),
    )
    .with_buffer_size(Some(20 * 1024 * 1024))
    .build()?;

    // Iterate over 20mb chunks of the output_file
    let mut buf = vec![0u8; 20 * 1024 * 1024];
    loop {
        match buf_reader.read(&mut buf) {
            Ok(0) => break, // End of file
            Ok(size) => {
                object_writer.write_all(&buf[..size]).await?;
            }
            Err(e) => return Err(DataFusionError::External(Box::new(e))),
        }
    }

    object_writer.shutdown().await?;
    Ok(())
}

/// Factory for creating GeoJSON file formats
#[derive(Default, Debug)]
pub struct GeoJsonFileFactory {
    file_factory: GeoJsonFormatFactory,
}

impl GeoJsonFileFactory {
    pub fn new() -> Self {
        Self {
            file_factory: GeoJsonFormatFactory::new(),
        }
    }
}

impl FileFormatFactory for GeoJsonFileFactory {
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

impl GetExt for GeoJsonFileFactory {
    fn get_ext(&self) -> String {
        self.file_factory.get_ext()
    }
}
