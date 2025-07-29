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
use std::collections::HashSet;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use datafusion::common::Statistics;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{
    FileMeta, FileOpenFuture, FileOpener, FileScanConfig, FileSource,
};
use datafusion::error::Result;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use futures::StreamExt;
use geoarrow_flatgeobuf::reader::{FlatGeobufReaderOptions, FlatGeobufStreamBuilder};
use geoarrow_schema::{CoordType, GeoArrowType};
use object_store::ObjectStore;

#[derive(Debug, Clone, Default)]
pub struct FlatGeobufSource {
    batch_size: Option<usize>,
    coord_type: CoordType,
    file_schema: Option<SchemaRef>,
    projection: Option<Vec<usize>>,
    metrics: ExecutionPlanMetricsSet,
    projected_statistics: Option<Statistics>,
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

        let mut options = FlatGeobufReaderOptions {
            batch_size: self.config.batch_size.unwrap_or(1024),
            coord_type: self.config.coord_type,
            ..Default::default()
        };

        let file_schema = self.config.file_schema.as_ref().unwrap();
        if let Some(projection) = &self.config.projection {
            options.read_geometry = false;
            let mut columns = HashSet::new();
            for projection_idx in projection {
                let field = file_schema.field(*projection_idx);
                if field
                    .extension_type_name()
                    .is_some_and(|name| name.starts_with("geoarrow") || name.starts_with("ogc"))
                    && GeoArrowType::try_from(field).is_ok()
                {
                    options.read_geometry = true;
                } else {
                    columns.insert(field.name().clone());
                }
            }

            options.columns = Some(columns);
        }

        Ok(Box::pin(async move {
            let reader =
                FlatGeobufStreamBuilder::new_from_store(store, file_meta.location().clone())
                    .await
                    .unwrap();
            let stream = reader.read(options).await.unwrap();
            Ok(stream.boxed())
        }))
    }
}
