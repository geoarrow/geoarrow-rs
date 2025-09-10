use std::any::Any;
use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_array::cast::AsArray;
use arrow_array::types::Float64Type;
use arrow_schema::{DataType, Field, FieldRef};
use datafusion::common::scalar::ScalarStructBuilder;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::function::AccumulatorArgs;
use datafusion::logical_expr::{Accumulator, AggregateUDFImpl, Signature};
use datafusion::scalar::ScalarValue;
use geoarrow_array::array::from_arrow_array;
use geoarrow_schema::{BoxType, Dimension, Metadata};

use crate::data_types::any_single_geometry_type_input;
use crate::error::GeoDataFusionResult;
use crate::udf::native::bounding_box::util::bounds::{BoundingRect, total_bounds};

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Extent {
    signature: Signature,
}

impl Extent {
    pub fn new() -> Self {
        Self {
            signature: any_single_geometry_type_input(),
        }
    }
}

impl Default for Extent {
    fn default() -> Self {
        Self::new()
    }
}

impl AggregateUDFImpl for Extent {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_extent"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field(&self, arg_fields: &[FieldRef]) -> Result<FieldRef> {
        Ok(return_field_impl(arg_fields)?)
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let input_field = acc_args.exprs[0].return_field(acc_args.schema)?;
        Ok(Box::new(ExtentAccumulator::new(input_field)))
    }
}

fn return_field_impl(args: &[FieldRef]) -> GeoDataFusionResult<FieldRef> {
    let metadata = Arc::new(Metadata::try_from(args[0].as_ref()).unwrap_or_default());
    let output_type = BoxType::new(Dimension::XY, metadata);
    Ok(Arc::new(output_type.to_field("", true)))
}

#[derive(Debug)]
struct ExtentAccumulator {
    inner: BoundingRect,
    field: FieldRef,
}

impl ExtentAccumulator {
    pub fn new(field: FieldRef) -> Self {
        Self {
            inner: BoundingRect::new(false),
            field,
        }
    }
}

impl Accumulator for ExtentAccumulator {
    /// Intermediate state of the accumulator.
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::from(self.inner.minx()),
            ScalarValue::from(self.inner.miny()),
            ScalarValue::from(self.inner.maxx()),
            ScalarValue::from(self.inner.maxy()),
        ])
    }

    /// Finish the accumulator
    fn evaluate(&mut self) -> Result<ScalarValue> {
        ScalarStructBuilder::new()
            .with_scalar(
                Field::new("xmin", DataType::Float64, false),
                self.inner.minx().into(),
            )
            .with_scalar(
                Field::new("ymin", DataType::Float64, false),
                self.inner.miny().into(),
            )
            .with_scalar(
                Field::new("xmax", DataType::Float64, false),
                self.inner.maxx().into(),
            )
            .with_scalar(
                Field::new("ymax", DataType::Float64, false),
                self.inner.maxy().into(),
            )
            .build()
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let geo_arr = from_arrow_array(&values[0], &self.field).unwrap();
        let bounds = total_bounds(&geo_arr).unwrap();
        self.inner.update(&bounds);
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let states_minx = arrow_arith::aggregate::min(states[0].as_primitive::<Float64Type>())
            .unwrap_or(f64::MAX);
        let states_miny = arrow_arith::aggregate::min(states[1].as_primitive::<Float64Type>())
            .unwrap_or(f64::MAX);
        let states_maxx = arrow_arith::aggregate::max(states[2].as_primitive::<Float64Type>())
            .unwrap_or(f64::MIN);
        let states_maxy = arrow_arith::aggregate::max(states[3].as_primitive::<Float64Type>())
            .unwrap_or(f64::MIN);

        self.inner.minx = self.inner.minx.min(states_minx);
        self.inner.miny = self.inner.miny.min(states_miny);
        self.inner.maxx = self.inner.maxx.max(states_maxx);
        self.inner.maxy = self.inner.maxy.max(states_maxy);

        Ok(())
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
}

#[cfg(test)]
mod test {
    // use arrow_array::cast::AsArray;
    // use arrow_array::types::Float64Type;
    use datafusion::prelude::SessionContext;

    use super::*;
    use crate::udf::native::io::GeomFromText;

    #[tokio::test]
    async fn test() {
        let ctx = SessionContext::new();

        ctx.register_udaf(Extent::new().into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());

        let df = ctx
            .sql(
                "select ST_Extent(ST_GeomFromText('POLYGON((743238 2967416,743238 2967450,
				 743265 2967450,743265.625 2967416,743238 2967416))'));",
            )
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let col = batch.column(0);
        let struct_arr = col.as_struct();
        assert_eq!(
            struct_arr.column(0).as_primitive::<Float64Type>().value(0),
            743238.0
        );
        assert_eq!(
            struct_arr.column(1).as_primitive::<Float64Type>().value(0),
            2967416.0
        );
        assert_eq!(
            struct_arr.column(2).as_primitive::<Float64Type>().value(0),
            743265.625
        );
        assert_eq!(
            struct_arr.column(3).as_primitive::<Float64Type>().value(0),
            2967450.0
        );
    }
}
