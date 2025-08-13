// #![doc = include_str!("../README.md")]
#![cfg_attr(docsrs, feature(doc_auto_cfg))]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]
// #![warn(missing_docs)]
#![doc(
    html_logo_url = "https://github.com/geoarrow.png",
    html_favicon_url = "https://github.com/geoarrow.png?size=32"
)]

pub mod file_format;
pub mod source;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::SessionContext;
    use geodatafusion::udf::geo::processing::Centroid;

    use crate::file_format::GeoParquetFormatFactory;

    #[tokio::test]
    async fn test_geoparquet() {
        let file_format = Arc::new(GeoParquetFormatFactory::default());
        let state = SessionStateBuilder::new()
            .with_file_formats(vec![file_format])
            .build();
        let ctx = SessionContext::new_with_state(state).enable_url_table();
        ctx.register_udf(Centroid::new(Default::default()).into());

        let df = ctx
            .sql("SELECT ST_Centroid(geometry) FROM '../../fixtures/geoparquet/nybb_wkb.parquet' as table")
            .await
            .unwrap();
        df.show().await.unwrap();
    }

    #[tokio::test]
    async fn test_geoparquet_geoarrow_metadata() {
        let file_format = Arc::new(GeoParquetFormatFactory::default());
        let state = SessionStateBuilder::new()
            .with_file_formats(vec![file_format])
            .build();
        let ctx = SessionContext::new_with_state(state).enable_url_table();

        let df = ctx
            .sql("SELECT geometry FROM '../../fixtures/geoparquet/nybb_wkb.parquet' as table LIMIT 10")
            .await
            .unwrap();
        let schema = df.schema();
        let field = schema.field_with_unqualified_name("geometry").unwrap();
        assert_eq!(field.extension_type_name().unwrap(), "geoarrow.wkb");
    }
}
