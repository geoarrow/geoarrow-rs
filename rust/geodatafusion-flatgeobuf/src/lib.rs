#![cfg_attr(docsrs, feature(doc_auto_cfg))]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]
#![doc(
    html_logo_url = "https://github.com/geoarrow.png",
    html_favicon_url = "https://github.com/geoarrow.png?size=32"
)]

pub mod file_format;
pub mod source;
mod sink;
mod utils;

pub use file_format::{FlatGeobufFileFactory, FlatGeobufFormat, FlatGeobufFormatFactory};
pub use sink::FlatGeobufSink;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::AsArray;
    use arrow_array::RecordBatch;
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::datasource::listing::{
        ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
    };
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::SessionContext;
    use geoarrow_array::GeoArrowArrayAccessor;
    use geoarrow_array::array::MultiPolygonArray;
    use geoarrow_schema::CoordType;
    use geodatafusion::udf::geo::processing::Centroid;

    use super::*;

    #[tokio::test]
    async fn test_flatgeobuf_format() {
        let file_format = Arc::new(FlatGeobufFileFactory::new(CoordType::default(), true));
        let state = SessionStateBuilder::new()
            .with_file_formats(vec![file_format])
            .build();
        let ctx = SessionContext::new_with_state(state);

        // Create the ListingTableConfig from local file
        let config = ListingTableConfig::new(
            ListingTableUrl::parse("../../fixtures/flatgeobuf/countries.fgb").unwrap(),
        )
        .infer(&ctx.state())
        .await
        .unwrap();

        // Build the ListingTable
        let table = ListingTable::try_new(config).unwrap();

        // Register under a name
        ctx.register_table("countries", Arc::new(table)).unwrap();

        ctx.register_udf(Centroid::new(Default::default()).into());

        let df = ctx
            .sql("SELECT ST_Centroid(geometry) as centroid, * FROM countries WHERE name = 'Zambia'")
            .await
            .unwrap();
        // .collect()
        // .await.unwrap();
        df.show().await.unwrap();
    }

    #[tokio::test]
    async fn test_column_projection_with_geometry() {
        let file_format = Arc::new(FlatGeobufFileFactory::default());
        let state = SessionStateBuilder::new()
            .with_file_formats(vec![file_format])
            .build();
        let ctx = SessionContext::new_with_state(state);

        // Create the ListingTableConfig from local file
        let config = ListingTableConfig::new(
            ListingTableUrl::parse("../../fixtures/flatgeobuf/countries.fgb").unwrap(),
        )
        .with_listing_options(ListingOptions::new(Arc::new(FlatGeobufFormat::default())))
        .infer_schema(&ctx.state())
        .await
        .unwrap();

        // Build the ListingTable
        let table = ListingTable::try_new(config).unwrap();

        // Register under a name
        ctx.register_table("countries", Arc::new(table)).unwrap();

        let df = ctx
            .sql("SELECT name, geometry FROM countries WHERE name = 'Zambia'")
            .await
            .unwrap();
        let schema = df.schema().clone();
        let batches = df.collect().await.unwrap();

        let batch = batches.into_iter().next().unwrap();
        let name_column = batch.column_by_name("name").unwrap();
        assert_eq!(name_column.as_string_view().value(0), "Zambia");

        let geometry_field = schema.field_with_unqualified_name("geometry").unwrap();
        let geometry_column = batch.column_by_name("geometry").unwrap();
        let geometry_array =
            MultiPolygonArray::try_from((geometry_column.as_ref(), geometry_field)).unwrap();
        let _polygon = geometry_array.value(0).unwrap();
    }

    #[tokio::test]
    async fn test_column_projection_without_geometry() {
        let file_format = Arc::new(FlatGeobufFileFactory::default());
        let state = SessionStateBuilder::new()
            .with_file_formats(vec![file_format])
            .build();
        let ctx = SessionContext::new_with_state(state);

        // Create the ListingTableConfig from local file
        let config = ListingTableConfig::new(
            ListingTableUrl::parse("../../fixtures/flatgeobuf/countries.fgb").unwrap(),
        )
        .with_listing_options(ListingOptions::new(Arc::new(FlatGeobufFormat::default())))
        .infer_schema(&ctx.state())
        .await
        .unwrap();

        // Build the ListingTable
        let table = ListingTable::try_new(config).unwrap();

        // Register under a name
        ctx.register_table("countries", Arc::new(table)).unwrap();

        let batches = ctx
            .sql("SELECT id FROM countries WHERE name = 'Zambia'")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let batch = batches.into_iter().next().unwrap();
        let id_column = batch.column_by_name("id").unwrap();
        assert_eq!(id_column.as_string_view().value(0), "ZMB");
    }

    fn sample_table() -> (Vec<RecordBatch>, Arc<Schema>) {
        use arrow_array::{Int32Array, RecordBatch};
        use geo_types::Coord;
        use geoarrow_array::builder::PointBuilder;
        use geoarrow_array::GeoArrowArray;
        use geoarrow_schema::{Dimension, PointType};

        let point_type = PointType::new(Dimension::XY, Default::default());
        let mut builder = PointBuilder::new(point_type);
        builder
            .try_push_coord(Some(&Coord { x: 0.0, y: 1.0 }))
            .unwrap();
        builder
            .try_push_coord(Some(&Coord { x: 1.0, y: 2.0 }))
            .unwrap();
        let geometry: Arc<dyn GeoArrowArray> = Arc::new(builder.finish());

        let fields = vec![
            Arc::new(Field::new("id", DataType::Int32, true)),
            Arc::new(geometry.data_type().to_field("geometry", true)),
        ];
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])) as _ ,
                geometry.into_array_ref(),
            ],
        )
        .unwrap();

        (vec![batch], schema)
    }

    #[tokio::test]
    async fn test_write_flatgeobuf_sink() {
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
        use datafusion_datasource::file_groups::FileGroup;
        use datafusion_datasource::sink::DataSink;
        use datafusion_datasource::PartitionedFile;
        use datafusion_datasource::file_sink_config::FileSinkConfig;
        use datafusion::datasource::listing::ListingTableUrl;
        use datafusion::execution::{SendableRecordBatchStream, TaskContext, object_store::ObjectStoreUrl};
        use datafusion::logical_expr::dml::InsertOp;

        let (batches, schema) = sample_table();
        let file_path = std::env::temp_dir().join("test_fgb_sink.fgb");
        let path_str = file_path.to_str().unwrap().to_string();

        let partitioned_file = PartitionedFile::new(path_str.clone(), 0);
        let file_group = FileGroup::new(vec![partitioned_file]);

        let config = FileSinkConfig {
            original_url: path_str.clone(),
            object_store_url: ObjectStoreUrl::local_filesystem(),
            file_group,
            table_paths: vec![ListingTableUrl::parse(path_str.clone()).unwrap()],
            output_schema: schema.clone(),
            table_partition_cols: vec![],
            insert_op: InsertOp::Append,
            keep_partition_by_columns: false,
            file_extension: "fgb".to_string(),
        };

        let sink = FlatGeobufSink::new(config);
        let context = Arc::new(TaskContext::default());

        let stream = futures::stream::iter(batches.clone().into_iter().map(Ok));
        let sendable_stream: SendableRecordBatchStream =
            Box::pin(RecordBatchStreamAdapter::new(schema.clone(), stream));

        sink.write_all(sendable_stream, &context).await.unwrap();

        let file = std::fs::File::open(&file_path).unwrap();
        let reader = std::io::BufReader::new(file);
        let fgb_reader = flatgeobuf::FgbReader::open(reader).unwrap();
        assert_eq!(fgb_reader.header().features_count(), 2);

        std::fs::remove_file(&file_path).unwrap();
    }
}
