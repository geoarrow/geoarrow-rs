// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

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
use geoarrow_flatgeobuf::reader::schema::FlatGeobufSchemaScanner;
use geoarrow_flatgeobuf::reader::{
    FlatGeobufHeaderExt, FlatGeobufReaderOptions, FlatGeobufRecordBatchStream,
};
use geoarrow_schema::CoordType;
use object_store::ObjectStore;

use crate::utils::open_flatgeobuf_reader;

#[derive(Debug, Clone, Default)]
pub struct FlatGeobufSource {
    batch_size: Option<usize>,
    coord_type: CoordType,
    file_schema: Option<SchemaRef>,
    projection: Option<Vec<usize>>,
    metrics: ExecutionPlanMetricsSet,
    projected_statistics: Option<Statistics>,
    prefer_view_types: bool,
    max_read_records: Option<usize>,
}

impl FlatGeobufSource {
    pub fn new() -> Self {
        Self {
            batch_size: None,
            coord_type: CoordType::default(),
            file_schema: None,
            projection: None,
            metrics: ExecutionPlanMetricsSet::new(),
            projected_statistics: None,
            prefer_view_types: true,
            max_read_records: Some(1000),
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
        let store2 = Arc::clone(&self.object_store);

        let _file_schema = self.config.file_schema.as_ref().unwrap();
        // if let Some(projection) = &self.config.projection {
        //     options.read_geometry = false;
        //     let mut columns = HashSet::new();
        //     for projection_idx in projection {
        //         let field = file_schema.field(*projection_idx);
        //         if field
        //             .extension_type_name()
        //             .is_some_and(|name| name.starts_with("geoarrow") || name.starts_with("ogc"))
        //             && GeoArrowType::try_from(field).is_ok()
        //         {
        //             options.read_geometry = true;
        //         } else {
        //             columns.insert(field.name().clone());
        //         }
        //     }

        //     options.columns = Some(columns);
        // }

        let config = self.config.clone();

        Ok(Box::pin(async move {
            let fgb_reader = open_flatgeobuf_reader(store, file_meta.location().clone()).await?;
            let fgb_header = fgb_reader.header();

            let geometry_type = fgb_header
                .geoarrow_type(config.coord_type)
                .map_err(|err| DataFusionError::External(Box::new(err)))?;
            let mut properties_schema =
                if let Some(schema) = fgb_header.properties_schema(config.prefer_view_types) {
                    schema
                } else {
                    // Scan to infer schema
                    let mut schema_builder = FlatGeobufSchemaScanner::new(config.prefer_view_types);

                    // Open a separate reader solely for schema inference
                    let schema_scanner_reader =
                        open_flatgeobuf_reader(store2, file_meta.location().clone()).await?;
                    let scan_selection = schema_scanner_reader
                        .select_all()
                        .await
                        .map_err(|err| DataFusionError::External(Box::new(err)))?;
                    schema_builder
                        .process_async(scan_selection, config.max_read_records)
                        .await
                        .map_err(|err| DataFusionError::External(Box::new(err)))?;
                    schema_builder.finish()
                };

            if let Some(projection) = config.projection.clone() {
                properties_schema = Arc::new(properties_schema.project(&projection)?);
            }

            let options = FlatGeobufReaderOptions::new(properties_schema, geometry_type)
                .with_batch_size(config.batch_size.unwrap_or(1024));

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
