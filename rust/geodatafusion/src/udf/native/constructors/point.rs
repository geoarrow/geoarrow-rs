//! Point constructors

use std::any::Any;
use std::sync::OnceLock;

use arrow_array::Array;
use arrow_array::cast::AsArray;
use arrow_array::types::Float64Type;
use arrow_schema::{DataType, Field};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use geoarrow_array::GeoArrowArray;
use geoarrow_array::array::{PointArray, SeparatedCoordBuffer};
use geoarrow_array::builder::PointBuilder;
use geoarrow_schema::{CoordType, Dimension, PointType};

use crate::error::GeoDataFusionResult;

#[derive(Debug)]
pub struct Point {
    signature: Signature,
    coord_type: CoordType,
}

impl Point {
    pub fn new(coord_type: CoordType) -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Float64, DataType::Float64],
                Volatility::Immutable,
            ),
            coord_type,
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let x = arrays[0].as_primitive::<Float64Type>();
        let y = arrays[1].as_primitive::<Float64Type>();

        // This passes
        assert_eq!(
            args.return_field.extension_type_name(),
            Some("geoarrow.point")
        );

        let typ = args.return_field.extension_type::<PointType>();
        let point_arr = match typ.coord_type() {
            CoordType::Interleaved => {
                let mut builder = PointBuilder::with_capacity(typ, x.len());
                for (x, y) in x.iter().zip(y.iter()) {
                    if let (Some(x), Some(y)) = (x, y) {
                        builder.push_coord(Some(&geo::coord! { x: x, y: y}));
                    } else {
                        builder.push_null();
                    }
                }

                builder.finish()
            }
            CoordType::Separated => {
                let (_, dim, metadata) = typ.into_inner();
                let coords = SeparatedCoordBuffer::from_vec(
                    vec![x.values().clone(), y.values().clone()],
                    dim,
                )?;
                PointArray::new(coords.into(), x.nulls().cloned(), metadata)
            }
        };

        Ok(point_arr.into_array_ref().into())
    }
}

static POINT_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Point {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_point"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<Field> {
        let typ = PointType::new(self.coord_type, Dimension::XY, Default::default());
        let field = typ.to_field("", true);

        // This passes
        assert_eq!(field.extension_type_name(), Some("geoarrow.point"));
        Ok(field)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(self.invoke_with_args(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(POINT_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns a Point with the given X and Y coordinate values.",
                "ST_Point(-71.104, 42.315)",
            )
            .with_argument("x", "x value")
            .with_argument("y", "y value")
            .with_related_udf("st_makepoint")
            .with_related_udf("st_pointz")
            .build()
        }))
    }
}

// #[derive(Debug)]
// pub(super) struct MakePoint {
//     signature: Signature,
// }

// impl MakePoint {
//     pub fn new() -> Self {
//         Self {
//             signature: Signature::one_of(
//                 vec![
//                     TypeSignature::Exact(vec![DataType::Float64, DataType::Float64]),
//                     TypeSignature::Exact(vec![
//                         DataType::Float64,
//                         DataType::Float64,
//                         DataType::Float64,
//                     ]),
//                 ],
//                 Volatility::Immutable,
//             ),
//         }
//     }
// }

// static MAKE_POINT_DOC: OnceLock<Documentation> = OnceLock::new();

// struct PointZ {
//     x: f64,
//     y: f64,
//     z: f64,
// }

// impl CoordTrait for PointZ {
//     type T = f64;

//     fn dim(&self) -> geo_traits::Dimensions {
//         geo_traits::Dimensions::Xyz
//     }

//     fn x(&self) -> Self::T {
//         self.x
//     }

//     fn y(&self) -> Self::T {
//         self.y
//     }

//     fn nth_or_panic(&self, n: usize) -> Self::T {
//         match n {
//             0 => self.x,
//             1 => self.y,
//             2 => self.z,
//             _ => panic!("invalid dimension index"),
//         }
//     }
// }

// impl ScalarUDFImpl for MakePoint {
//     fn as_any(&self) -> &dyn Any {
//         self
//     }

//     fn name(&self) -> &str {
//         "st_makepoint"
//     }

//     fn signature(&self) -> &Signature {
//         &self.signature
//     }

//     fn return_type(&self, arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
//         match arg_types.len() {
//             2 => Ok(POINT2D_TYPE().into()),
//             3 => Ok(POINT3D_TYPE().into()),
//             _ => unreachable!(),
//         }
//     }

//     fn invoke_with_args(
//         &self,
//         args: ScalarFunctionArgs,
//     ) -> datafusion::error::Result<ColumnarValue> {
//         let mut args = ColumnarValue::values_to_arrays(&args.args)?.into_iter();
//         let x = args.next().unwrap();
//         let y = args.next().unwrap();
//         let z = args.next();

//         let x = x.as_primitive::<Float64Type>();
//         let y = y.as_primitive::<Float64Type>();

//         let dim = if z.is_some() {
//             Dimension::XYZ
//         } else {
//             Dimension::XY
//         };
//         let typ = PointType::new(CoordType::Separated, dim, Default::default());
//         let mut builder = PointBuilder::with_capacity(typ, x.len());

//         if let Some(z) = z {
//             let z = z.as_primitive::<Float64Type>();

//             for ((x, y), z) in x.iter().zip(y.iter()).zip(z.iter()) {
//                 if let (Some(x), Some(y), Some(z)) = (x, y, z) {
//                     builder.push_coord(Some(&PointZ { x, y, z }));
//                 } else {
//                     builder.push_null();
//                 }
//             }
//         } else {
//             for (x, y) in x.iter().zip(y.iter()) {
//                 if let (Some(x), Some(y)) = (x, y) {
//                     builder.push_coord(Some(&geo::coord! { x: x, y: y}));
//                 } else {
//                     builder.push_null();
//                 }
//             }
//         }

//         Ok(builder.finish().into_array_ref().into())
//     }

//     fn documentation(&self) -> Option<&Documentation> {
//         Some(MAKE_POINT_DOC.get_or_init(|| {
//             Documentation::builder(
//                 DOC_SECTION_OTHER,
//                 "Creates a 2D XY or 3D XYZ Point geometry.",
//                 "ST_MakePoint(-71.104, 42.315)",
//             )
//             .with_argument("x", "x value")
//             .with_argument("y", "y value")
//             .with_argument("z", "z value")
//             .with_related_udf("st_point")
//             .with_related_udf("st_pointz")
//             .build()
//         }))
//     }
// }

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use approx::relative_eq;
    use arrow_array::{RecordBatch, create_array};
    use arrow_schema::Schema;
    use datafusion::prelude::SessionContext;
    use geo_traits::{CoordTrait, PointTrait};
    use geoarrow_array::ArrayAccessor;

    use super::*;

    #[ignore = "UDFs on scalar input not yet supported (see https://github.com/geoarrow/geoarrow-rs/pull/1106#issuecomment-2866322000)"]
    #[tokio::test]
    async fn test_st_point() {
        let ctx = SessionContext::new();

        ctx.register_udf(Point::new(CoordType::Separated).into());

        let sql_df = ctx
            .sql(r#"SELECT ST_Point(-71.104, 42.315);"#)
            .await
            .unwrap();

        let output_batches = sql_df.collect().await.unwrap();
        assert_eq!(output_batches.len(), 1);
        let output_batch = &output_batches[0];
        let output_schema = output_batch.schema();
        let output_field = output_schema.field(0);
        dbg!(output_field);

        // This fails
        assert_eq!(output_field.extension_type_name(), Some("geoarrow.point"));

        // let output_column = output_batch.column(0);
        // let point_arr = PointArray::try_from((output_column.as_ref(), output_field)).unwrap();

        // assert_eq!(point_arr.len(), 1);
        // let (x, y) = point_arr.value(0).unwrap().coord().unwrap().x_y();

        // assert!(relative_eq!(x, -71.104));
        // assert!(relative_eq!(y, 42.315));
    }

    #[tokio::test]
    async fn test_st_point_from_table() {
        let ctx = SessionContext::new();

        ctx.register_udf(Point::new(CoordType::Separated).into());

        let x = create_array!(Float64, [-71.104]);
        let y = create_array!(Float64, [42.315]);

        let schema = Schema::new([
            Arc::new(Field::new("x", x.data_type().clone(), true)),
            Arc::new(Field::new("y", y.data_type().clone(), true)),
        ]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![x, y]).unwrap();

        ctx.register_batch("t", batch).unwrap();

        let sql_df = ctx.sql(r#"SELECT ST_Point(x, y) from t;"#).await.unwrap();

        let output_batches = sql_df.collect().await.unwrap();
        assert_eq!(output_batches.len(), 1);
        let output_batch = &output_batches[0];
        let output_schema = output_batch.schema();
        let output_field = output_schema.field(0);

        // This succeeds
        assert_eq!(output_field.extension_type_name(), Some("geoarrow.point"));

        let output_column = output_batch.column(0);
        let point_arr = PointArray::try_from((output_column.as_ref(), output_field)).unwrap();

        assert_eq!(point_arr.len(), 1);
        let (x, y) = point_arr.value(0).unwrap().coord().unwrap().x_y();

        assert!(relative_eq!(x, -71.104));
        assert!(relative_eq!(y, 42.315));
    }
}
