use std::any::Any;
use std::sync::{Arc, OnceLock};

use arrow_array::StringArrayType;
use arrow_array::cast::AsArray;
use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use geoarrow_array::GeoArrowArray;
use geoarrow_array::array::RectArray;
use geoarrow_array::builder::RectBuilder;
use geoarrow_schema::{BoxType, Dimension, Metadata};

use crate::error::GeoDataFusionResult;

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Box2DFromGeoHash {
    signature: Signature,
}

impl Box2DFromGeoHash {
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(
                1,
                vec![DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View],
                Volatility::Immutable,
            ),
        }
    }
}

impl Default for Box2DFromGeoHash {
    fn default() -> Self {
        Self::new()
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Box2DFromGeoHash {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_box2dfromgeohash"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(return_field_impl(args)?)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(box_from_geohash_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Return a BOX2D from a GeoHash string.",
                "ST_Box2dFromGeoHash(geohash)",
            )
            .with_argument("text", "geohash")
            .build()
        }))
    }
}

fn return_field_impl(args: ReturnFieldArgs) -> GeoDataFusionResult<FieldRef> {
    let metadata = Arc::new(Metadata::try_from(args.arg_fields[0].as_ref()).unwrap_or_default());
    let output_type = BoxType::new(Dimension::XY, metadata);
    Ok(Arc::new(output_type.to_field("", true)))
}

fn box_from_geohash_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let array = ColumnarValue::values_to_arrays(&args.args)?
        .into_iter()
        .next()
        .unwrap();

    let typ = args.return_field.extension_type::<BoxType>();

    let rect_arr = match array.data_type() {
        DataType::Utf8 => build_rect_arr(typ, &array.as_string::<i32>()),
        DataType::LargeUtf8 => build_rect_arr(typ, &array.as_string::<i64>()),
        DataType::Utf8View => build_rect_arr(typ, &array.as_string_view()),
        _ => unreachable!(),
    }?;

    Ok(ColumnarValue::Array(rect_arr.into_array_ref()))
}

fn build_rect_arr<'a>(
    typ: BoxType,
    array: &impl StringArrayType<'a>,
) -> GeoDataFusionResult<RectArray> {
    let mut builder = RectBuilder::with_capacity(typ, array.len());
    for s in array.iter() {
        builder.push_rect(s.map(geohash::decode_bbox).transpose()?.as_ref());
    }
    Ok(builder.finish())
}

#[cfg(test)]
mod tests {
    use approx::relative_eq;
    use datafusion::prelude::SessionContext;
    use geo_traits::{CoordTrait, RectTrait};
    use geoarrow_array::GeoArrowArrayAccessor;

    use super::*;

    #[tokio::test]
    async fn test_box2d_from_geohash() {
        let ctx = SessionContext::new();
        ctx.register_udf(Box2DFromGeoHash::default().into());

        let df = ctx
            .sql("SELECT ST_Box2dFromGeoHash('ww8p1r4t8');")
            .await
            .unwrap();

        let schema = df.schema().clone();
        let batches = df.collect().await.unwrap();
        let column = batches[0].column(0);

        let rect_array = RectArray::try_from((column.as_ref(), schema.field(0))).unwrap();
        let rect = rect_array.value(0).unwrap();

        assert!(relative_eq!(rect.min().x(), 112.55836486816406));
        assert!(relative_eq!(rect.min().y(), 37.83236503601074));
        assert!(relative_eq!(rect.max().x(), 112.5584077835083));
        assert!(relative_eq!(rect.max().y(), 37.83240795135498));
    }
}
