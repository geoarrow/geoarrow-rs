#![cfg_attr(docsrs, feature(doc_auto_cfg))]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]
#![doc(
    html_logo_url = "https://github.com/geoarrow.png",
    html_favicon_url = "https://github.com/geoarrow.png?size=32"
)]

pub mod file_format;
pub mod source;
mod utils;

pub use file_format::{FlatGeobufFileFactory, FlatGeobufFormat, FlatGeobufFormatFactory};

#[cfg(test)]
mod tests {
    use std::env::temp_dir;
    use std::fs::File;
    use std::io::BufReader;
    use std::sync::Arc;

    use arrow_array::{Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::arrow::array::AsArray;
    use datafusion::catalog::MemTable;
    use datafusion::datasource::listing::{
        ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
    };
    use datafusion::execution::SessionStateBuilder;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::prelude::SessionContext;
    use geoarrow_array::array::MultiPolygonArray;
    use geoarrow_array::builder::PointBuilder;
    use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};
    use geoarrow_schema::{CoordType, Dimension, PointType};
    use geodatafusion::udf::geo::processing::Centroid;
    use wkt::wkt;

    use super::*;

    #[tokio::test]
    async fn test_flatgeobuf_format() {
        let file_format = Arc::new(FlatGeobufFileFactory::new(CoordType::default(), true));
        let state = SessionStateBuilder::new()
            .with_file_formats(vec![file_format])
            .build();
        let ctx = SessionContext::new_with_state(state);

        let object_store_url = ObjectStoreUrl::parse("https://flatgeobuf.org").unwrap();
        let object_store = Arc::new(
            object_store::http::HttpBuilder::new()
                .with_url(object_store_url.as_str())
                .build()
                .unwrap(),
        );

        ctx.register_object_store(object_store_url.as_ref(), object_store);

        // Create the ListingTableConfig
        let config = ListingTableConfig::new(
            ListingTableUrl::parse("https://flatgeobuf.org/test/data/countries.fgb").unwrap(),
        )
        // .with_listing_options(ListingOptions::new(Arc::new(FlatGeobufFormat::default())))
        .infer(&ctx.state())
        // .infer_schema(&ctx.state())
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

        let object_store_url = ObjectStoreUrl::parse("https://flatgeobuf.org").unwrap();
        let object_store = Arc::new(
            object_store::http::HttpBuilder::new()
                .with_url(object_store_url.as_str())
                .build()
                .unwrap(),
        );

        ctx.register_object_store(object_store_url.as_ref(), object_store);

        // Create the ListingTableConfig
        let config = ListingTableConfig::new(
            ListingTableUrl::parse("https://flatgeobuf.org/test/data/countries.fgb").unwrap(),
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

        let object_store_url = ObjectStoreUrl::parse("https://flatgeobuf.org").unwrap();
        let object_store = Arc::new(
            object_store::http::HttpBuilder::new()
                .with_url(object_store_url.as_str())
                .build()
                .unwrap(),
        );

        ctx.register_object_store(object_store_url.as_ref(), object_store);

        // Create the ListingTableConfig
        let config = ListingTableConfig::new(
            ListingTableUrl::parse("https://flatgeobuf.org/test/data/countries.fgb").unwrap(),
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
        let mut builder = PointBuilder::new(PointType::new(Dimension::XY, Default::default()));
        builder.push_point(Some(&wkt!( POINT(1.0 2.0) )));
        builder.push_point(Some(&wkt!( POINT(2.0 3.0) )));
        let geometry = builder.finish();

        let fields = vec![
            Arc::new(Field::new("id", DataType::Int32, true)),
            Arc::new(geometry.data_type().to_field("geometry", true)),
        ];
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])) as _,
                geometry.into_array_ref(),
            ],
        )
        .unwrap();

        (vec![batch], schema)
    }

    #[tokio::test]
    async fn test_write_flatgeobuf_sink() {
        let file_format = Arc::new(FlatGeobufFileFactory::default());
        let state = SessionStateBuilder::new()
            .with_file_formats(vec![file_format])
            .build();
        let ctx = SessionContext::new_with_state(state);

        let (batches, schema) = sample_table();
        let mem_table = Arc::new(MemTable::try_new(schema.clone(), vec![batches]).unwrap());
        ctx.register_table("mem_table", mem_table).unwrap();

        let file_path = temp_dir().join("test_fgb_sink.fgb");

        ctx.sql(&format!("COPY mem_table TO '{}';", file_path.display(),))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let file = File::open(&file_path).unwrap();
        let reader = BufReader::new(file);
        let fgb_reader = flatgeobuf::FgbReader::open(reader).unwrap();
        assert_eq!(fgb_reader.header().features_count(), 2);

        std::fs::remove_file(&file_path).unwrap();
    }
}
