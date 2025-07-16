use std::any::Any;
use std::sync::{Arc, OnceLock};

use arrow_schema::DataType;
use datafusion::error::Result;
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use geoarrow_array::array::from_arrow_array;

use crate::error::GeoDataFusionResult;

#[derive(Debug)]
pub struct Distance {
    signature: Signature,
}

impl Distance {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl Default for Distance {
    fn default() -> Self {
        Self::new()
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Distance {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_distance"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(distance_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
                    Documentation::builder(DOC_SECTION_OTHER, "For geometry types returns the minimum 2D Cartesian (planar) distance between two geometries, in projected units (spatial ref units).", "ST_Distance(geomA, geomB)")
                        .with_argument("geomA", "geometry")
                        .with_argument("geomB", "geometry")
                        .build()
                }))
    }
}

fn distance_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let left_arr = from_arrow_array(&arrays[0], &args.arg_fields[0])?;
    let right_arr = from_arrow_array(&arrays[1], &args.arg_fields[1])?;
    let result = geoarrow_geo::euclidean_distance(&left_arr, &right_arr)?;
    Ok(ColumnarValue::Array(Arc::new(result)))
}

#[cfg(test)]
mod test {
    use approx::assert_relative_eq;
    use arrow_array::cast::AsArray;
    use arrow_array::types::Float64Type;
    use datafusion::prelude::SessionContext;

    use super::*;
    use crate::udf::native::io::GeomFromText;

    #[tokio::test]
    async fn test_distance() {
        let ctx = SessionContext::new();

        ctx.register_udf(Distance::new().into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());

        let df = ctx
            .sql("SELECT ST_Distance(ST_GeomFromText('POINT(-72.1235 42.3521)'), ST_GeomFromText('LINESTRING(-72.1260 42.45, -72.123 42.1546)'));")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let col = batch.column(0);
        assert_relative_eq!(
            col.as_primitive::<Float64Type>().value(0),
            0.00150567726382282
        );
    }
}
