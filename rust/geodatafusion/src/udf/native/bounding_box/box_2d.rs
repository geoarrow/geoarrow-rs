use std::any::Any;
use std::sync::OnceLock;

use arrow_schema::DataType;
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{ColumnarValue, Documentation, ScalarUDFImpl, Signature};
use geoarrow::ArrayBase;
use geoarrow::algorithm::native::BoundingRectArray;

use crate::data_types::{BOX2D_TYPE, any_single_geometry_type_input, parse_to_native_array};
use crate::error::GeoDataFusionResult;

#[derive(Debug)]
pub(super) struct Box2D {
    signature: Signature,
}

impl Box2D {
    pub fn new() -> Self {
        Self {
            signature: any_single_geometry_type_input(),
        }
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Box2D {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_box2d"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(BOX2D_TYPE().into())
    }

    fn invoke(&self, args: &[ColumnarValue]) -> datafusion::error::Result<ColumnarValue> {
        Ok(box2d_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns a box2d representing the 2D extent of the geometry.",
                "ST_Box2D(geometry)",
            )
            .with_argument("geom", "geometry")
            .build()
        }))
    }
}

// Note: this is exactly the same impl as ST_Envelope. Perhaps we should use an alias instead
fn box2d_impl(args: &[ColumnarValue]) -> GeoDataFusionResult<ColumnarValue> {
    let array = ColumnarValue::values_to_arrays(args)?
        .into_iter()
        .next()
        .unwrap();
    let native_array = parse_to_native_array(array)?;
    let output = native_array.as_ref().bounding_rect()?;
    Ok(output.into_array_ref().into())
}
