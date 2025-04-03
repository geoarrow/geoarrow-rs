use std::any::Any;
use std::sync::OnceLock;

use arrow::array::AsArray;
use arrow::datatypes::Float64Type;
use arrow_schema::DataType;
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, Volatility,
};
use geoarrow::algorithm::broadcasting::BroadcastablePrimitive;
use geoarrow::algorithm::geo::SimplifyVw as _;

use crate::data_types::{parse_to_native_array, GEOMETRY_TYPE};
use crate::error::GeoDataFusionResult;

#[derive(Debug)]
pub(super) struct SimplifyVw {
    signature: Signature,
}

impl SimplifyVw {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![GEOMETRY_TYPE().into(), DataType::Float64],
                Volatility::Immutable,
            ),
        }
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for SimplifyVw {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_simplifyvw"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn invoke(&self, args: &[ColumnarValue]) -> datafusion::error::Result<ColumnarValue> {
        Ok(simplify_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns a simplified representation of a geometry using the Visvalingam-Whyatt algorithm. The simplification tolerance is an area value, in the units of the input SRS. Simplification removes vertices which form \"corners\" with area less than the tolerance. The result may not be valid even if the input is.

The function can be called with any kind of geometry (including GeometryCollections), but only line and polygon elements are simplified. Endpoints of linear geometry are preserved.",
                "ST_SimplifyVW(geometry, epsilon)",
            )
            .with_argument("geom", "geometry")
            .with_argument("tolerance", "float")
            .build()
        }))
    }
}

fn simplify_impl(args: &[ColumnarValue]) -> GeoDataFusionResult<ColumnarValue> {
    let array = ColumnarValue::values_to_arrays(&args[..1])?
        .into_iter()
        .next()
        .unwrap();
    let native_array = parse_to_native_array(array)?;
    let output = match &args[1] {
        ColumnarValue::Scalar(epsilon) => {
            let epsilon = epsilon.to_scalar()?.into_inner();
            let epsilon = epsilon.as_primitive::<Float64Type>().value(0);
            native_array.as_ref().simplify_vw(&epsilon.into())?
        }
        ColumnarValue::Array(epsilon) => {
            native_array
                .as_ref()
                .simplify_vw(&BroadcastablePrimitive::Array(
                    epsilon.as_primitive().clone(),
                ))?
        }
    };
    Ok(output.to_array_ref().into())
}

#[cfg(test)]
mod test {
    use datafusion::prelude::*;
    use geo::line_string;
    use geoarrow::array::GeometryArray;
    use geoarrow::trait_::ArrayAccessor;

    use crate::udf::native::register_native;

    #[ignore = "Union fields length must match child arrays length"]
    #[tokio::test]
    async fn test() {
        let ctx = SessionContext::new();
        register_native(&ctx);

        let out = ctx.sql(
            "SELECT ST_SimplifyVW(ST_GeomFromText('LINESTRING(5 2, 3 8, 6 20, 7 25, 10 10)'), 30);").await.unwrap();
        let batches = out.collect().await.unwrap();
        let column = batches.first().unwrap().columns().first().unwrap().clone();
        let geom_arr = GeometryArray::try_from(column.as_ref()).unwrap();
        let expected = line_string![
            (x: 5.0, y: 2.0),
            (x: 7.0, y: 25.0),
            (x: 10.0, y: 10.0),
        ];
        let expected = geo::Geometry::LineString(expected);
        assert_eq!(geom_arr.value_as_geo(0), expected);
    }
}
