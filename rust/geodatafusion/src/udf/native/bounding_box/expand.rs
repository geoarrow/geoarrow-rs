use std::any::Any;
use std::sync::OnceLock;

use arrow::array::AsArray;
use arrow::datatypes::Float64Type;
use arrow_schema::DataType;
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use geo_traits::{CoordTrait, RectTrait};
use geoarrow::ArrayBase;
use geoarrow::array::{RectArray, RectBuilder};
use geoarrow::error::GeoArrowError;
use geoarrow::trait_::ArrayAccessor;
use geoarrow_schema::Dimension;

use crate::data_types::BOX2D_TYPE;
use crate::error::GeoDataFusionResult;

#[derive(Debug)]
pub(super) struct Expand {
    signature: Signature,
}

impl Expand {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![BOX2D_TYPE().into(), DataType::Float64]),
                    TypeSignature::Exact(vec![
                        BOX2D_TYPE().into(),
                        DataType::Float64,
                        DataType::Float64,
                    ]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Expand {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_expand"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(arg_types.first().unwrap().clone())
    }

    fn invoke(&self, args: &[ColumnarValue]) -> datafusion::error::Result<ColumnarValue> {
        Ok(expand_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns a bounding box expanded from the bounding box of the input, either by specifying a single distance with which the box should be expanded on both axes, or by specifying an expansion distance for each axis. Uses double-precision. Can be used for distance queries, or to add a bounding box filter to a query to take advantage of a spatial index.",
                "ST_Expand(box)",
            )
            .with_argument("box", "box2d")
            .build()
        }))
    }
}

fn expand_impl(args: &[ColumnarValue]) -> GeoDataFusionResult<ColumnarValue> {
    let mut args = ColumnarValue::values_to_arrays(args)?.into_iter();
    let rect_array = args.next().unwrap();
    let factor1 = args.next().unwrap();
    let factor2 = args.next();

    let dx = factor1.as_primitive::<Float64Type>();

    if BOX2D_TYPE()
        .to_data_type()
        .equals_datatype(rect_array.data_type())
    {
        let rect_array = RectArray::try_from((rect_array.as_ref(), Dimension::XY))?;
        let mut builder = RectBuilder::with_capacity_and_options(
            Dimension::XY,
            rect_array.len(),
            rect_array.metadata().clone(),
        );

        if let Some(dy) = factor2 {
            let dy = dy.as_primitive::<Float64Type>();

            for val in rect_array.iter().zip(dx.iter()).zip(dy.iter()) {
                if let ((Some(rect), Some(dx)), Some(dy)) = val {
                    builder.push_rect(Some(&expand_2d_rect(rect, dx, dy)));
                } else {
                    builder.push_null();
                }
            }
        } else {
            for val in rect_array.iter().zip(dx.iter()) {
                if let (Some(rect), Some(dx)) = val {
                    builder.push_rect(Some(&expand_2d_rect(rect, dx, dx)));
                } else {
                    builder.push_null();
                }
            }
        }

        return Ok(builder.finish().into_array_ref().into());
    }

    Err(Err(GeoArrowError::General(format!(
        "Unexpected data type: {:?}",
        rect_array.data_type()
    )))?)
}

#[inline]
fn expand_2d_rect(rect: impl RectTrait<T = f64>, dx: f64, dy: f64) -> geo::Rect<f64> {
    let min = rect.min();
    let max = rect.max();

    let new_min = geo::coord! { x: min.x() - dx, y: min.y() - dy };
    let new_max = geo::coord! { x: max.x() + dx, y: max.y() + dy };

    geo::Rect::new(new_min, new_max)
}

#[cfg(test)]
mod test {
    use approx::relative_eq;
    use datafusion::prelude::*;
    use geo_traits::{CoordTrait, RectTrait};
    use geoarrow::array::RectArray;
    use geoarrow::trait_::ArrayAccessor;

    use crate::data_types::BOX2D_TYPE;
    use crate::udf::native::register_native;

    use super::*;

    #[tokio::test]
    async fn test() {
        let ctx = SessionContext::new();
        register_native(&ctx);

        let out = ctx
            .sql("SELECT ST_Expand(ST_MakeBox2D(ST_Point(0, 5), ST_Point(10, 20)), 10, 20);")
            .await
            .unwrap();

        let batches = out.collect().await.unwrap();
        assert_eq!(batches.len(), 1);
        let batch = batches.into_iter().next().unwrap();
        assert_eq!(batch.columns().len(), 1);
        assert!(
            batch
                .schema()
                .field(0)
                .data_type()
                .equals_datatype(&BOX2D_TYPE().into())
        );

        let rect_array = RectArray::try_from((batch.columns()[0].as_ref(), Dimension::XY)).unwrap();
        let rect = rect_array.value(0);

        assert!(relative_eq!(rect.min().x(), -10.0));
        assert!(relative_eq!(rect.min().y(), -15.0));
        assert!(relative_eq!(rect.max().x(), 20.0));
        assert!(relative_eq!(rect.max().y(), 40.0));
    }
}
