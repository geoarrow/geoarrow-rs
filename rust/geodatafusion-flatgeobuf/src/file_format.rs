use std::any::Any;
use std::collections::HashMap;
use std::fmt::{self, Debug, Formatter};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::sync::Arc;

use arrow_schema::{Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::catalog::memory::DataSourceExec;
use datafusion::common::runtime::SpawnedTask;
use datafusion::common::stats::Precision;
use datafusion::common::{GetExt, Statistics};
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::file_format::{FileFormat, FileFormatFactory};
use datafusion::datasource::physical_plan::{FileScanConfig, FileScanConfigBuilder, FileSource};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::LexRequirement;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan};
use datafusion_datasource::display::FileGroupDisplay;
use datafusion_datasource::file_sink_config::{FileSink, FileSinkConfig};
use datafusion_datasource::sink::{DataSink, DataSinkExec};
use datafusion_datasource::write::ObjectWriterBuilder;
use datafusion_datasource::write::demux::DemuxedStreamReceiver;
use geoarrow_flatgeobuf::reader::FlatGeobufHeaderExt;
use geoarrow_flatgeobuf::reader::schema::FlatGeobufSchemaScanner;
use geoarrow_flatgeobuf::writer::{FlatGeobufWriter, FlatGeobufWriterOptions};
use geoarrow_schema::{CoordType, GeoArrowType};
use object_store::path::Path;
use object_store::{ObjectMeta, ObjectStore};
use tempfile::NamedTempFile;
use tokio::io::AsyncWriteExt;

use crate::source::FlatGeobufSource;
use crate::utils::open_flatgeobuf_reader;

/// Factory used to create [`FlatGeobufFormat`]
#[derive(Debug)]
pub struct FlatGeobufFormatFactory {
    coord_type: CoordType,
    use_view_types: bool,
    max_scan_records: Option<usize>,
}

impl FlatGeobufFormatFactory {
    /// Creates an instance of [`FlatGeobufFormatFactory`]
    pub fn new(coord_type: CoordType, use_view_types: bool) -> Self {
        Self {
            coord_type,
            use_view_types,
            max_scan_records: Some(1000),
        }
    }
}

impl Default for FlatGeobufFormatFactory {
    fn default() -> Self {
        Self {
            coord_type: CoordType::default(),
            use_view_types: true,
            max_scan_records: Some(1000),
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
            use_view_types: self.use_view_types,
            max_scan_records: self.max_scan_records,
        }))
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        Arc::new(FlatGeobufFormat {
            coord_type: self.coord_type,
            use_view_types: self.use_view_types,
            max_scan_records: self.max_scan_records,
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
    use_view_types: bool,
    max_scan_records: Option<usize>,
}

impl Default for FlatGeobufFormat {
    fn default() -> Self {
        dbg!("default impl on FlatGeobufFormat");
        Self {
            coord_type: CoordType::default(),
            use_view_types: true,
            max_scan_records: Some(1000),
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
    use_view_types: bool,
    max_scan_records: Option<usize>,
) -> Result<(SchemaRef, GeoArrowType)> {
    let reader = open_flatgeobuf_reader(store, location).await?;
    let header = reader.header();
    let geometry_type = header
        .geoarrow_type(coord_type)
        .map_err(|err| DataFusionError::External(Box::new(err)))?;
    if let Some(schema) = reader.header().properties_schema(use_view_types) {
        Ok((schema, geometry_type))
    } else {
        // Scan to infer schema
        let mut schema_builder = FlatGeobufSchemaScanner::new(use_view_types);
        let selection = reader
            .select_all()
            .await
            .map_err(|err| DataFusionError::External(Box::new(err)))?;
        schema_builder
            .process_async(selection, max_scan_records)
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
        FlatGeobufFormatFactory::new(Default::default(), Default::default()).get_ext()
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
                self.use_view_types,
                self.max_scan_records,
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

    async fn create_writer_physical_plan(
        &self,
        input: Arc<dyn ExecutionPlan>,
        _state: &dyn Session,
        conf: FileSinkConfig,
        order_requirements: Option<LexRequirement>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let sink = Arc::new(FlatGeobufSink::new(conf));
        Ok(Arc::new(DataSinkExec::new(input, sink, order_requirements)))
    }

    fn file_source(&self) -> Arc<dyn FileSource> {
        Arc::new(FlatGeobufSource::default())
    }

    /// Returns whether this instance uses compression if applicable
    fn compression_type(&self) -> Option<FileCompressionType> {
        Some(FileCompressionType::UNCOMPRESSED)
    }
}

#[derive(Debug)]
pub struct FlatGeobufSink {
    config: FileSinkConfig,
}

impl FlatGeobufSink {
    pub fn new(config: FileSinkConfig) -> Self {
        Self { config }
    }
}

impl DisplayAs for FlatGeobufSink {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter<'_>) -> fmt::Result {
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
        let mut total_rows: u64 = 0;
        while let Some((path, mut rb_rx)) = file_stream_rx.recv().await {
            // We create a tempfile on disk because the FlatGeobufWriter is sync only. So we write
            // to a temp file and then upload it to the object store.
            let named_temp_file = NamedTempFile::new()?;
            let output_file = BufWriter::new(named_temp_file);

            // Create FlatGeobufWriter
            let name = path
                .filename()
                .map(|s| s.to_string().rsplit_once(".").unwrap().0.to_string())
                .unwrap_or_else(|| "file".to_string());
            let options = FlatGeobufWriterOptions::new(name);
            let mut fgb_writer = FlatGeobufWriter::try_new(
                output_file,
                self.config.output_schema().clone(),
                options,
            )
            .map_err(|err| DataFusionError::External(Box::new(err)))?;

            // For each record batch received, write it to the FlatGeobufWriter
            while let Some(batch) = rb_rx.recv().await {
                total_rows += batch.num_rows() as u64;
                fgb_writer
                    .write(&batch)
                    .map_err(|err| DataFusionError::External(Box::new(err)))?;
            }

            // Finalize the FlatGeobufWriter to the temp file
            let mut output_file = fgb_writer
                .finish()
                .map_err(|err| DataFusionError::External(Box::new(err)))?;
            output_file.flush()?;

            let named_temp_file = output_file
                .into_inner()
                .map_err(|err| DataFusionError::External(Box::new(err)))?;

            // Reopen the temp file for reading
            let mut buf_reader = BufReader::new(File::open(named_temp_file.path())?);

            let mut object_writer = ObjectWriterBuilder::new(
                FileCompressionType::UNCOMPRESSED,
                &path,
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
        }
        demux_task
            .join()
            .await
            .map_err(|e| DataFusionError::Execution(e.to_string()))??;
        Ok(total_rows)
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

/// Factory for creating FlatGeobuf file formats
#[derive(Default, Debug)]
pub struct FlatGeobufFileFactory {
    file_factory: FlatGeobufFormatFactory,
}

impl FlatGeobufFileFactory {
    pub fn new(coord_type: CoordType, use_view_types: bool) -> Self {
        Self {
            file_factory: FlatGeobufFormatFactory::new(coord_type, use_view_types),
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
