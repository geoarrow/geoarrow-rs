#![cfg_attr(not(test), warn(unused_crate_dependencies))]

pub mod file_format;
pub mod source;
mod utils;

pub use file_format::{FlatGeobufFileFactory, FlatGeobufFormat, FlatGeobufFormatFactory};

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::AsArray;
    use datafusion::datasource::listing::{
        ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
    };
    use datafusion::execution::SessionStateBuilder;
    use datafusion::execution::object_store::ObjectStoreUrl;
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
}
