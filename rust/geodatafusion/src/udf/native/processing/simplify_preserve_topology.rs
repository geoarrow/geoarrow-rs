use std::any::Any;
use std::sync::OnceLock;

use arrow_array::cast::AsArray;
use arrow::datatypes::Float64Type;
use arrow_schema::DataType;
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, Volatility,
};
use geoarrow::algorithm::broadcasting::BroadcastablePrimitive;
use geoarrow::algorithm::geo::SimplifyVwPreserve as _;

use crate::data_types::{GEOMETRY_TYPE, parse_to_native_array};
use crate::error::GeoDataFusionResult;

#[derive(Debug)]
pub(super) struct SimplifyPreserveTopology {
    signature: Signature,
}

impl SimplifyPreserveTopology {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![GEOMETRY_TYPE().into(), DataType::Float64],
                Volatility::Immutable,
            ),
        }
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for SimplifyPreserveTopology {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_simplifypreservetopology"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn invoke(&self, args: &[ColumnarValue]) -> datafusion::error::Result<ColumnarValue> {
        Ok(simplify_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Computes a simplified representation of a geometry using a variant of the Visvalingam-Whyatt algorithm which limits simplification to ensure the result has the same topology as the input. The simplification tolerance is a distance value, in the units of the input SRS. Simplification removes vertices which are within the tolerance distance of the simplified linework, as long as topology is preserved. The result will be valid and simple if the input is.

The function can be called with any kind of geometry (including GeometryCollections), but only line and polygon elements are simplified. For polygonal inputs, the result will have the same number of rings (shells and holes), and the rings will not cross. Ring endpoints may be simplified. For linear inputs, the result will have the same number of lines, and lines will not intersect if they did not do so in the original geometry. Endpoints of linear geometry are preserved.",
                "ST_SimplifyPreserveTopology(geometry, epsilon)",
            )
            .with_argument("geom", "geometry")
            .with_argument("tolerance", "float")
            .build()
        }))
    }
}

fn simplify_impl(args: &[ColumnarValue]) -> GeoDataFusionResult<ColumnarValue> {
    let array = ColumnarValue::values_to_arrays(&args[..1])?
        .into_iter()
        .next()
        .unwrap();
    let native_array = parse_to_native_array(array)?;
    let output = match &args[1] {
        ColumnarValue::Scalar(epsilon) => {
            let epsilon = epsilon.to_scalar()?.into_inner();
            let epsilon = epsilon.as_primitive::<Float64Type>().value(0);
            native_array
                .as_ref()
                .simplify_vw_preserve(&epsilon.into())?
        }
        ColumnarValue::Array(epsilon) => {
            native_array
                .as_ref()
                .simplify_vw_preserve(&BroadcastablePrimitive::Array(
                    epsilon.as_primitive().clone(),
                ))?
        }
    };
    Ok(output.to_array_ref().into())
}
