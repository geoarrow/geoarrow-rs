use std::any::Any;
use std::sync::{Arc, OnceLock};

use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use geoarrow_array::GeoArrowArray;
use geoarrow_array::array::from_arrow_array;
use geoarrow_schema::{CoordType, Dimension, GeoArrowType, PointType};

use crate::data_types::any_single_geometry_type_input;
use crate::error::GeoDataFusionResult;

#[derive(Debug)]
pub struct PointOnSurface {
    signature: Signature,
    coord_type: CoordType,
}

impl PointOnSurface {
    pub fn new(coord_type: CoordType) -> Self {
        Self {
            signature: any_single_geometry_type_input(),
            coord_type,
        }
    }
}

impl Default for PointOnSurface {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for PointOnSurface {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_pointonsurface"
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
        Ok(interior_point_impl(args, self.coord_type)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns a POINT which is guaranteed to lie in the interior of a surface.",
                "ST_PointOnSurface(geometry)",
            )
            .with_argument("g1", "geometry")
            .build()
        }))
    }
}

fn return_field_impl(
    args: ReturnFieldArgs,
    coord_type: CoordType,
) -> GeoDataFusionResult<FieldRef> {
    let input_type = GeoArrowType::try_from(args.arg_fields[0].as_ref())?;
    let output_type =
        PointType::new(Dimension::XY, input_type.metadata().clone()).with_coord_type(coord_type);
    Ok(Arc::new(output_type.to_field("", true)))
}

fn interior_point_impl(
    args: ScalarFunctionArgs,
    coord_type: CoordType,
) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let geo_array = from_arrow_array(&arrays[0], &args.arg_fields[0])?;
    let result = geoarrow_geo::interior_point(&geo_array, coord_type)?;
    Ok(ColumnarValue::Array(result.into_array_ref()))
}
