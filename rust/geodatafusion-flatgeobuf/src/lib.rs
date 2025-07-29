#![cfg_attr(not(test), warn(unused_crate_dependencies))]

pub mod file_format;
pub mod source;

pub use file_format::{FlatGeobufFileFactory, FlatGeobufFormat, FlatGeobufFormatFactory};

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::datasource::listing::{
        ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
    };
    use datafusion::error::Result;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::prelude::SessionContext;
    use geodatafusion::udf::native::measurement::Centroid;

    use super::*;

    #[tokio::test]
    async fn test_flatgeobuf_format() {
        main().await.unwrap();
    }

    async fn main() -> Result<()> {
        // Create a new context with the default configuration
        let mut state = SessionStateBuilder::new().with_default_features().build();

        // Register the custom file format
        let file_format = Arc::new(FlatGeobufFileFactory::new());
        state.register_file_format(file_format, true).unwrap();

        // Create a new context with the custom file format
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
        let config = ListingTableConfig::new(ListingTableUrl::parse(
            "https://flatgeobuf.org/test/data/countries.fgb",
        )?)
        .with_listing_options(ListingOptions::new(Arc::new(FlatGeobufFormat::default())))
        .infer_schema(&ctx.state())
        .await?;

        // Build the ListingTable
        let table = ListingTable::try_new(config)?;

        // Register under a name
        ctx.register_table("countries", Arc::new(table))?;

        ctx.register_udf(Centroid::new(Default::default()).into());

        let df = ctx
            .sql("SELECT ST_Centroid(geometry) as centroid, * FROM countries WHERE name = 'Zambia'")
            .await?;
        // .collect()
        // .await?;
        df.show().await?;

        // let mem_table = create_mem_table();
        // ctx.register_table("mem_table", mem_table).unwrap();

        // let temp_dir = tempdir().unwrap();
        // let table_save_path = temp_dir.path().join("mem_table.tsv");

        // let d = ctx
        //     .sql(&format!(
        //         "COPY mem_table TO '{}' STORED AS TSV;",
        //         table_save_path.display(),
        //     ))
        //     .await?;

        // let results = d.collect().await?;
        // println!(
        //     "Number of inserted rows: {:?}",
        //     (results[0]
        //         .column_by_name("count")
        //         .unwrap()
        //         .as_primitive::<UInt64Type>()
        //         .value(0))
        // );

        Ok(())
    }

    // // create a simple mem table
    // fn create_mem_table() -> Arc<MemTable> {
    //     let fields = vec![
    //         Field::new("id", DataType::UInt8, false),
    //         Field::new("data", DataType::Utf8, false),
    //     ];
    //     let schema = Arc::new(Schema::new(fields));

    //     let partitions = RecordBatch::try_new(
    //         schema.clone(),
    //         vec![
    //             Arc::new(UInt8Array::from(vec![1, 2])),
    //             Arc::new(StringArray::from(vec!["foo", "bar"])),
    //         ],
    //     )
    //     .unwrap();

    //     Arc::new(MemTable::try_new(schema, vec![vec![partitions]]).unwrap())
    // }
}
