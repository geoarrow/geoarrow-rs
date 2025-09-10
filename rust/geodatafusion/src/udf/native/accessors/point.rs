use std::any::Any;
use std::sync::{Arc, OnceLock};

use arrow_array::Float64Array;
use arrow_array::builder::Float64Builder;
use arrow_schema::DataType;
use datafusion::error::Result;
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use geo_traits::{CoordTrait, GeometryTrait, PointTrait};
use geoarrow_array::array::from_arrow_array;
use geoarrow_array::{GeoArrowArrayAccessor, downcast_geoarrow_array};

use crate::data_types::any_point_type_input;
use crate::error::GeoDataFusionResult;

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct X {
    signature: Signature,
}

impl X {
    pub fn new() -> Self {
        Self {
            signature: any_point_type_input(1),
        }
    }
}

impl Default for X {
    fn default() -> Self {
        Self::new()
    }
}

static X_DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for X {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_x"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(nth_impl(args, 0)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(X_DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Return the X coordinate of the point, or NULL if not available. Input must be a point.",
                "ST_X(geometry)",
            )
            .with_argument("a_point", "geometry")
            .build()
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Y {
    signature: Signature,
}

impl Y {
    pub fn new() -> Self {
        Self {
            signature: any_point_type_input(1),
        }
    }
}

impl Default for Y {
    fn default() -> Self {
        Self::new()
    }
}

static Y_DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Y {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_y"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(nth_impl(args, 1)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(Y_DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Return the Y coordinate of the point, or NULL if not available. Input must be a point.",
                "ST_Y(geometry)",
            )
            .with_argument("a_point", "geometry")
            .build()
        }))
    }
}

fn nth_impl(args: ScalarFunctionArgs, n: usize) -> GeoDataFusionResult<ColumnarValue> {
    let array = ColumnarValue::values_to_arrays(&args.args)?
        .into_iter()
        .next()
        .unwrap();
    let geo_array = from_arrow_array(array.as_ref(), args.arg_fields[0].as_ref())?;
    let geo_ref = geo_array.as_ref();
    let result = downcast_geoarrow_array!(geo_ref, _nth_impl, n)?;
    Ok(ColumnarValue::Array(Arc::new(result)))
}

fn _nth_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    n: usize,
) -> GeoDataFusionResult<Float64Array> {
    let mut builder = Float64Builder::with_capacity(array.len());
    for geom in array.iter() {
        if let Some(geo_geom) = geom {
            match geo_geom?.as_type() {
                geo_traits::GeometryType::Point(point) => {
                    builder.append_option(point.coord().and_then(|c| c.nth(n)));
                }
                _ => {
                    builder.append_null();
                }
            }
        } else {
            builder.append_null();
        }
    }
    Ok(builder.finish())
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Z {
    signature: Signature,
}

impl Z {
    pub fn new() -> Self {
        Self {
            signature: any_point_type_input(1),
        }
    }
}

impl Default for Z {
    fn default() -> Self {
        Self::new()
    }
}

static Z_DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Z {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_z"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(nth_impl(args, 2)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(Z_DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Return the Z coordinate of the point, or NULL if not available. Input must be a point.",
                "ST_Z(geometry)",
            )
            .with_argument("a_point", "geometry")
            .build()
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct M {
    signature: Signature,
}

impl M {
    pub fn new() -> Self {
        Self {
            signature: any_point_type_input(1),
        }
    }
}

impl Default for M {
    fn default() -> Self {
        Self::new()
    }
}

static M_DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for M {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_m"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(m_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(M_DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Return the M coordinate of the point, or NULL if not available. Input must be a point.",
                "ST_M(geometry)",
            )
            .with_argument("a_point", "geometry")
            .build()
        }))
    }
}

fn m_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let array = ColumnarValue::values_to_arrays(&args.args)?
        .into_iter()
        .next()
        .unwrap();
    let geo_array = from_arrow_array(array.as_ref(), args.arg_fields[0].as_ref())?;
    let geo_ref = geo_array.as_ref();
    let result = downcast_geoarrow_array!(geo_ref, _m_impl)?;
    Ok(ColumnarValue::Array(Arc::new(result)))
}

fn _m_impl<'a>(array: &'a impl GeoArrowArrayAccessor<'a>) -> GeoDataFusionResult<Float64Array> {
    let mut builder = Float64Builder::with_capacity(array.len());
    for geom in array.iter() {
        if let Some(geo_geom) = geom {
            match geo_geom?.as_type() {
                geo_traits::GeometryType::Point(point) => {
                    builder.append_option(point.coord().and_then(|c| match c.dim() {
                        geo_traits::Dimensions::Xym => c.nth(2),
                        geo_traits::Dimensions::Xyzm => c.nth(3),
                        _ => None,
                    }));
                }
                _ => {
                    builder.append_null();
                }
            }
        } else {
            builder.append_null();
        }
    }
    Ok(builder.finish())
}

#[cfg(test)]
mod test {
    use arrow_array::cast::AsArray;
    use arrow_array::types::Float64Type;
    use datafusion::prelude::SessionContext;

    use super::*;
    use crate::udf::native::constructors::{PointM, PointZ, PointZM};
    use crate::udf::native::io::GeomFromText;

    #[tokio::test]
    async fn test_accessors() {
        let ctx = SessionContext::new();

        ctx.register_udf(X::new().into());
        ctx.register_udf(Y::new().into());
        ctx.register_udf(Z::new().into());
        ctx.register_udf(M::new().into());
        ctx.register_udf(PointZ::new(Default::default()).into());
        ctx.register_udf(PointM::new(Default::default()).into());
        ctx.register_udf(PointZM::new(Default::default()).into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());

        let df = ctx
            .sql("SELECT ST_X(ST_GeomFromText('POINT(1 2)'));")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        assert_eq!(batch.column(0).as_primitive::<Float64Type>().value(0), 1.0);

        let df = ctx
            .sql("SELECT ST_Y(ST_GeomFromText('POINT(1 2)'));")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        assert_eq!(batch.column(0).as_primitive::<Float64Type>().value(0), 2.0);

        let df = ctx.sql("SELECT ST_Z(ST_PointZ(1, 2, 3));").await.unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        assert_eq!(batch.column(0).as_primitive::<Float64Type>().value(0), 3.0);

        let df = ctx
            .sql("SELECT ST_Z(ST_PointZM(1, 2, 3, 4));")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        assert_eq!(batch.column(0).as_primitive::<Float64Type>().value(0), 3.0);

        let df = ctx.sql("SELECT ST_M(ST_PointM(1, 2, 3));").await.unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        assert_eq!(batch.column(0).as_primitive::<Float64Type>().value(0), 3.0);

        let df = ctx
            .sql("SELECT ST_M(ST_PointZM(1, 2, 3, 4));")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        assert_eq!(batch.column(0).as_primitive::<Float64Type>().value(0), 4.0);
    }
}
