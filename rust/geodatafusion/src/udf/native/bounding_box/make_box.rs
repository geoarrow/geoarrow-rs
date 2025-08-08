use std::any::Any;
use std::sync::{Arc, OnceLock};

use arrow_schema::{DataType, FieldRef};
use datafusion::arrow::buffer::NullBuffer;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use geoarrow_array::GeoArrowArray;
use geoarrow_array::array::{CoordBuffer, PointArray, RectArray};
use geoarrow_schema::{BoxType, CoordType, Dimension, Metadata, PointType};

use crate::error::GeoDataFusionResult;

#[derive(Debug)]
pub struct MakeBox2D {
    signature: Signature,
}

impl MakeBox2D {
    pub fn new() -> Self {
        let mut valid_types = vec![];

        for coord_type in [CoordType::Separated, CoordType::Interleaved] {
            valid_types.push(
                PointType::new(Dimension::XY, Default::default())
                    .with_coord_type(coord_type)
                    .data_type(),
            );
        }

        Self {
            signature: Signature::uniform(2, valid_types, Volatility::Immutable),
        }
    }
}

impl Default for MakeBox2D {
    fn default() -> Self {
        Self::new()
    }
}

static DOC_2D: OnceLock<Documentation> = OnceLock::new();

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

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        let output_type = BoxType::new(Dimension::XY, Arc::new(Metadata::default()));
        Ok(Arc::new(output_type.to_field("", true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(make_box_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOC_2D.get_or_init(|| {
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

#[derive(Debug)]
pub struct MakeBox3D {
    signature: Signature,
}

impl MakeBox3D {
    pub fn new() -> Self {
        let mut valid_types = vec![];

        for coord_type in [CoordType::Separated, CoordType::Interleaved] {
            valid_types.push(
                PointType::new(Dimension::XYZ, Default::default())
                    .with_coord_type(coord_type)
                    .data_type(),
            );
        }

        Self {
            signature: Signature::uniform(2, valid_types, Volatility::Immutable),
        }
    }
}

impl Default for MakeBox3D {
    fn default() -> Self {
        Self::new()
    }
}

static DOC_3D: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for MakeBox3D {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_3dmakebox"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        let output_type = BoxType::new(Dimension::XYZ, Arc::new(Metadata::default()));
        Ok(Arc::new(output_type.to_field("", true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(make_box_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOC_3D.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Creates a box3d defined by two 3D Point geometries.",
                "ST_3DMakeBox(ST_MakePoint(-989502.1875, 528439.5625, 10),
	ST_MakePoint(-987121.375 ,529933.1875, 10))",
            )
            .with_argument("pointLowLeft", "geometry")
            .with_argument("pointUpRight", "geometry")
            .build()
        }))
    }
}

fn make_box_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let mut arrays = ColumnarValue::values_to_arrays(&args.args)?.into_iter();
    let lower = arrays.next().unwrap();
    let upper = arrays.next().unwrap();

    let lower_type = args.arg_fields[0].try_extension_type::<PointType>()?;
    let upper_type = args.arg_fields[1].try_extension_type::<PointType>()?;

    let lower =
        PointArray::try_from((lower.as_ref(), lower_type))?.into_coord_type(CoordType::Separated);
    let upper =
        PointArray::try_from((upper.as_ref(), upper_type))?.into_coord_type(CoordType::Separated);

    let nulls = NullBuffer::union(
        lower.logical_nulls().as_ref(),
        upper.logical_nulls().as_ref(),
    );
    let lower_coords = match lower.coords() {
        CoordBuffer::Separated(coords) => coords,
        CoordBuffer::Interleaved(_) => unreachable!(),
    };
    let upper_coords = match upper.coords() {
        CoordBuffer::Separated(coords) => coords,
        CoordBuffer::Interleaved(_) => unreachable!(),
    };
    let rect_arr = RectArray::new(
        lower_coords.clone(),
        upper_coords.clone(),
        nulls,
        Arc::new(Metadata::default()),
    );

    Ok(rect_arr.into_array_ref().into())
}

#[cfg(test)]
mod test {
    use approx::relative_eq;
    use datafusion::prelude::*;
    use geo_traits::{CoordTrait, RectTrait};
    use geoarrow_array::GeoArrowArrayAccessor;

    use super::*;
    use crate::udf::native::constructors::{Point, PointZ};

    #[tokio::test]
    async fn test_2d() {
        let ctx = SessionContext::new();

        ctx.register_udf(MakeBox2D::new().into());
        ctx.register_udf(Point::default().into());

        let out = ctx
            .sql("SELECT ST_MakeBox2D(ST_Point(0, 5), ST_Point(10, 20));")
            .await
            .unwrap();
        let batch = out.collect().await.unwrap().into_iter().next().unwrap();
        let schema = batch.schema();
        let rect_array =
            RectArray::try_from((batch.columns()[0].as_ref(), schema.field(0))).unwrap();
        let rect = rect_array.value(0).unwrap();

        assert!(relative_eq!(rect.min().x(), 0.0));
        assert!(relative_eq!(rect.min().y(), 5.0));
        assert!(relative_eq!(rect.max().x(), 10.0));
        assert!(relative_eq!(rect.max().y(), 20.0));
    }

    #[tokio::test]
    async fn test_3d() {
        let ctx = SessionContext::new();

        ctx.register_udf(MakeBox3D::new().into());
        ctx.register_udf(PointZ::default().into());

        let out = ctx
            .sql("SELECT ST_3DMakeBox(ST_PointZ(0, 5, 1), ST_PointZ(10, 20, 30));")
            .await
            .unwrap();
        let batch = out.collect().await.unwrap().into_iter().next().unwrap();
        let schema = batch.schema();
        let rect_array =
            RectArray::try_from((batch.columns()[0].as_ref(), schema.field(0))).unwrap();
        let rect = rect_array.value(0).unwrap();

        assert!(relative_eq!(rect.min().x(), 0.0));
        assert!(relative_eq!(rect.min().y(), 5.0));
        assert!(relative_eq!(rect.min().nth_or_panic(2), 1.0));
        assert!(relative_eq!(rect.max().x(), 10.0));
        assert!(relative_eq!(rect.max().y(), 20.0));
        assert!(relative_eq!(rect.max().nth_or_panic(2), 30.0));
    }
}
