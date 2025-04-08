use std::any::Any;
use std::sync::OnceLock;

use arrow_schema::DataType;
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{ColumnarValue, Documentation, ScalarUDFImpl, Signature};
use geoarrow::ArrayBase;
use geoarrow::algorithm::geo::InteriorPoint;

use crate::data_types::{POINT2D_TYPE, any_single_geometry_type_input, parse_to_native_array};
use crate::error::GeoDataFusionResult;

#[derive(Debug)]
pub(super) struct PointOnSurface {
    signature: Signature,
}

impl PointOnSurface {
    pub fn new() -> Self {
        Self {
            signature: any_single_geometry_type_input(),
        }
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

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(POINT2D_TYPE().into())
    }

    fn invoke(&self, args: &[ColumnarValue]) -> datafusion::error::Result<ColumnarValue> {
        Ok(interior_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns a POINT which is guaranteed to lie in the interior of a surface (POLYGON, MULTIPOLYGON, and CURVEPOLYGON). In PostGIS this function also works on line and point geometries.",
                "ST_PointOnSurface(geometry)",
            )
            .with_argument("g1", "geometry")
            .build()
        }))
    }
}

fn interior_impl(args: &[ColumnarValue]) -> GeoDataFusionResult<ColumnarValue> {
    let array = ColumnarValue::values_to_arrays(args)?
        .into_iter()
        .next()
        .unwrap();
    let native_array = parse_to_native_array(array)?;
    let output = native_array.as_ref().interior_point()?;
    Ok(output.into_array_ref().into())
}
