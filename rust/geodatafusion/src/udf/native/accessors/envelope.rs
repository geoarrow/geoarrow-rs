use std::any::Any;
use std::sync::OnceLock;

use arrow_schema::DataType;
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{ColumnarValue, Documentation, ScalarUDFImpl, Signature};
use geoarrow::algorithm::native::BoundingRectArray;
use geoarrow::ArrayBase;

use crate::data_types::{any_single_geometry_type_input, parse_to_native_array, GEOMETRY_TYPE};
use crate::error::GeoDataFusionResult;

#[derive(Debug)]
pub(super) struct Envelope {
    signature: Signature,
}

impl Envelope {
    pub fn new() -> Self {
        Self {
            signature: any_single_geometry_type_input(),
        }
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Envelope {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_envelope"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(GEOMETRY_TYPE.into())
    }

    fn invoke(&self, args: &[ColumnarValue]) -> datafusion::error::Result<ColumnarValue> {
        Ok(envelope_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Computes a point which is the geometric center of mass of a geometry.",
                "ST_Envelope(geometry)",
            )
            .with_argument("g1", "geometry")
            .build()
        }))
    }
}

fn envelope_impl(args: &[ColumnarValue]) -> GeoDataFusionResult<ColumnarValue> {
    let array = ColumnarValue::values_to_arrays(args)?
        .into_iter()
        .next()
        .unwrap();
    let native_array = parse_to_native_array(array)?;
    // Since a RectArray is a valid normal geometry type for us, we don't have to cast it to a
    // Geometry array. That just has overhead.
    let output = native_array.as_ref().bounding_rect()?;
    Ok(output.into_array_ref().into())
}
