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
use geoarrow_schema::{BoxType, Dimension, Metadata};

use crate::data_types::any_single_geometry_type_input;
use crate::error::GeoDataFusionResult;
use crate::udf::native::bounding_box::util::bounds::bounding_rect;

#[derive(Debug)]
pub struct Box2D {
    signature: Signature,
}

impl Box2D {
    pub fn new() -> Self {
        Self {
            signature: any_single_geometry_type_input(),
        }
    }
}

impl Default for Box2D {
    fn default() -> Self {
        Self::new()
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Box2D {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "box2d"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(return_field_impl(args, Dimension::XY)?)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(box_impl(args, false)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns a box2d representing the 2D extent of the geometry.",
                "Box2D(geometry)",
            )
            .with_argument("geom", "geometry")
            .build()
        }))
    }
}

#[derive(Debug)]
pub struct Box3D {
    signature: Signature,
}

impl Box3D {
    pub fn new() -> Self {
        Self {
            signature: any_single_geometry_type_input(),
        }
    }
}

impl Default for Box3D {
    fn default() -> Self {
        Self::new()
    }
}

static DOC_3D: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Box3D {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "box3d"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(return_field_impl(args, Dimension::XYZ)?)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(box_impl(args, true)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOC_3D.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns a box3d representing the 3D extent of the geometry.",
                "Box3D(geometry)",
            )
            .with_argument("geom", "geometry")
            .build()
        }))
    }
}

fn return_field_impl(args: ReturnFieldArgs, dim: Dimension) -> GeoDataFusionResult<FieldRef> {
    let metadata = Arc::new(Metadata::try_from(args.arg_fields[0].as_ref()).unwrap_or_default());
    let output_type = BoxType::new(dim, metadata);
    Ok(Arc::new(output_type.to_field("", true)))
}

// Note: this is exactly the same impl as ST_Envelope. Perhaps we should use an alias instead
fn box_impl(args: ScalarFunctionArgs, include_z: bool) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let geo_array = from_arrow_array(&arrays[0], &args.arg_fields[0])?;
    let rect_array = bounding_rect(&geo_array, include_z)?;
    Ok(ColumnarValue::Array(rect_array.into_array_ref()))
}
