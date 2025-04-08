use std::any::Any;
use std::sync::OnceLock;

use arrow_schema::DataType;
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, Volatility,
};
use geo_traits::PointTrait;
use geoarrow::ArrayBase;
use geoarrow::array::{PointArray, RectBuilder};
use geoarrow::trait_::ArrayAccessor;
use geoarrow_schema::Dimension;

use crate::data_types::{BOX2D_TYPE, POINT2D_TYPE};
use crate::error::GeoDataFusionResult;

#[derive(Debug)]
pub(super) struct MakeBox2D {
    signature: Signature,
}

impl MakeBox2D {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![POINT2D_TYPE().into(), POINT2D_TYPE().into()],
                Volatility::Immutable,
            ),
        }
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for MakeBox2D {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_makebox2d"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(BOX2D_TYPE().into())
    }

    fn invoke(&self, args: &[ColumnarValue]) -> datafusion::error::Result<ColumnarValue> {
        Ok(make_box2d_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Creates a box2d defined by two Point geometries. This is useful for doing range queries.",
                "ST_MakeBox2D(ST_Point(-989502.1875, 528439.5625), ST_Point(-987121.375, 529933.1875))",
            )
            .with_argument("pointLowLeft", "geometry")
            .with_argument("pointUpRight", "geometry")
            .build()
        }))
    }
}

fn make_box2d_impl(args: &[ColumnarValue]) -> GeoDataFusionResult<ColumnarValue> {
    let mut args = ColumnarValue::values_to_arrays(args)?.into_iter();
    let min = args.next().unwrap();
    let max = args.next().unwrap();

    let min = PointArray::try_from((min.as_ref(), Dimension::XY))?;
    let max = PointArray::try_from((max.as_ref(), Dimension::XY))?;

    let mut builder =
        RectBuilder::with_capacity_and_options(Dimension::XY, min.len(), min.metadata().clone());

    for val in min.iter().zip(max.iter()) {
        if let (Some(min), Some(max)) = val {
            builder.push_min_max(&min.coord().unwrap(), &max.coord().unwrap());
        } else {
            builder.push_null();
        }
    }

    Ok(builder.finish().into_array_ref().into())
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
            .sql("SELECT ST_MakeBox2D(ST_Point(0, 5), ST_Point(10, 20));")
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

        assert!(relative_eq!(rect.min().x(), 0.0));
        assert!(relative_eq!(rect.min().y(), 5.0));
        assert!(relative_eq!(rect.max().x(), 10.0));
        assert!(relative_eq!(rect.max().y(), 20.0));
    }
}
