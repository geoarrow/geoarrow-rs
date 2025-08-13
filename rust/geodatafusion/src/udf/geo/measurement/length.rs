use std::any::Any;
use std::sync::{Arc, OnceLock};

use arrow_schema::DataType;
use datafusion::error::Result;
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use geoarrow_array::array::from_arrow_array;
use geoarrow_geo::euclidean_length;

use crate::data_types::any_single_geometry_type_input;
use crate::error::GeoDataFusionResult;

#[derive(Debug)]
pub struct Length {
    signature: Signature,
    aliases: Vec<String>,
}

impl Length {
    pub fn new() -> Self {
        Self {
            signature: any_single_geometry_type_input(),
            aliases: vec!["st_length2d".to_string()],
        }
    }
}

impl Default for Length {
    fn default() -> Self {
        Self::new()
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Length {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_length"
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(length_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the 2D Cartesian length of the geometry if it is a LineString or MultiLineString. For areal geometries 0 is returned; use ST_Perimeter instead.",
                "ST_Length(geom)",
            )
            .with_argument("geom", "geometry")
            .with_sql_example(
                "SELECT ST_Length(ST_GeomFromText('LINESTRING(0 0, 3 4)'));",
            )
            .build()
        }))
    }
}

fn length_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let array = ColumnarValue::values_to_arrays(&args.args)?
        .into_iter()
        .next()
        .unwrap();
    let field = &args.arg_fields[0];
    let geo_array = from_arrow_array(&array, field)?;
    let result = euclidean_length(&geo_array)?;
    Ok(ColumnarValue::Array(Arc::new(result)))
}

#[cfg(test)]
mod test {
    use arrow_array::cast::AsArray;
    use arrow_array::types::Float64Type;
    use datafusion::prelude::SessionContext;

    use super::*;
    use crate::udf::native::io::GeomFromText;

    #[tokio::test]
    async fn test_linestring_length() {
        let ctx = SessionContext::new();

        ctx.register_udf(Length::new().into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());

        let df = ctx
            .sql("SELECT ST_Length(ST_GeomFromText('LINESTRING(0 0, 3 4)'));")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let col = batch.column(0);
        let val = col.as_primitive::<Float64Type>().value(0);
        assert_eq!(val, 5.0);
    }

    #[tokio::test]
    async fn test_point_length() {
        let ctx = SessionContext::new();

        ctx.register_udf(Length::new().into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());

        let df = ctx
            .sql("SELECT ST_Length(ST_GeomFromText('POINT(1 2)'));")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let col = batch.column(0);
        let val = col.as_primitive::<Float64Type>().value(0);
        assert_eq!(val, 0.0);
    }

    #[tokio::test]
    async fn test_multilinestring_length() {
        let ctx = SessionContext::new();

        ctx.register_udf(Length::new().into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());

        let df = ctx
            .sql("SELECT ST_Length(ST_GeomFromText('MULTILINESTRING((0 0, 3 4), (0 0, 4 3))'));")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let col = batch.column(0);
        let val = col.as_primitive::<Float64Type>().value(0);
        assert_eq!(val, 10.0); // 5.0 + 5.0
    }

    #[tokio::test]
    async fn test_polygon_length() {
        let ctx = SessionContext::new();

        ctx.register_udf(Length::new().into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());

        let df = ctx
            .sql("SELECT ST_Length(ST_GeomFromText('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'));")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let col = batch.column(0);
        let val = col.as_primitive::<Float64Type>().value(0);
        assert_eq!(val, 0.0); // Polygons return 0 for length
    }
}
