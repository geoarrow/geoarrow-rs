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
use geoarrow_array::array::PointArray;
use geoarrow_array::builder::PointBuilder;
use geoarrow_schema::{CoordType, Dimension, Metadata, PointType};

use crate::error::GeoDataFusionResult;

#[derive(Debug)]
pub struct PointFromGeoHash {
    signature: Signature,
    coord_type: CoordType,
}

impl PointFromGeoHash {
    pub fn new(coord_type: CoordType) -> Self {
        Self {
            signature: Signature::uniform(
                1,
                vec![DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View],
                Volatility::Immutable,
            ),
            coord_type,
        }
    }
}

impl Default for PointFromGeoHash {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for PointFromGeoHash {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_pointfromgeohash"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(return_field_impl(args, self.coord_type)?)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(point_from_geohash_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Return a point from a GeoHash string. The point represents the center point of the GeoHash.",
                "ST_PointFromGeoHash(geohash)",
            )
            .with_argument("text", "geohash")
            .build()
        }))
    }
}

fn return_field_impl(
    args: ReturnFieldArgs,
    coord_type: CoordType,
) -> GeoDataFusionResult<FieldRef> {
    let metadata = Arc::new(Metadata::try_from(args.arg_fields[0].as_ref()).unwrap_or_default());
    let output_type = PointType::new(Dimension::XY, metadata).with_coord_type(coord_type);
    Ok(Arc::new(output_type.to_field("", true)))
}

fn point_from_geohash_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let array = ColumnarValue::values_to_arrays(&args.args)?
        .into_iter()
        .next()
        .unwrap();

    let typ = args.return_field.extension_type::<PointType>();
    let rect_arr = match array.data_type() {
        DataType::Utf8 => build_point_arr(typ, &array.as_string::<i32>()),
        DataType::LargeUtf8 => build_point_arr(typ, &array.as_string::<i64>()),
        DataType::Utf8View => build_point_arr(typ, &array.as_string_view()),
        _ => unreachable!(),
    }?;

    Ok(ColumnarValue::Array(rect_arr.into_array_ref()))
}

fn build_point_arr<'a>(
    typ: PointType,
    array: &impl StringArrayType<'a>,
) -> GeoDataFusionResult<PointArray> {
    let mut builder = PointBuilder::with_capacity(typ, array.len());
    for s in array.iter() {
        if let Some(s) = s {
            let (coord, _, _) = geohash::decode(s)?;
            builder.push_coord(Some(&coord));
        } else {
            builder.push_null();
        }
    }
    Ok(builder.finish())
}

#[cfg(test)]
mod tests {
    use approx::relative_eq;
    use datafusion::prelude::SessionContext;
    use geo_traits::{CoordTrait, PointTrait};
    use geoarrow_array::GeoArrowArrayAccessor;
    use geoarrow_array::array::PointArray;

    use super::*;

    #[tokio::test]
    async fn test_point_from_geohash() {
        let ctx = SessionContext::new();
        ctx.register_udf(PointFromGeoHash::default().into());

        let df = ctx
            .sql("SELECT ST_PointFromGeoHash('9qqj');")
            .await
            .unwrap();

        let schema = df.schema().clone();
        let batches = df.collect().await.unwrap();
        let column = batches[0].column(0);

        let point_array = PointArray::try_from((column.as_ref(), schema.field(0))).unwrap();
        let point = point_array.value(0).unwrap();

        assert!(relative_eq!(point.coord().unwrap().x(), -115.13671875));
        assert!(relative_eq!(point.coord().unwrap().y(), 36.123046875));
    }
}
