use std::any::Any;
use std::sync::{Arc, OnceLock};

use arrow_schema::DataType;
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{ColumnarValue, Documentation, ScalarUDFImpl, Signature};
use geoarrow::algorithm::geos::{BooleanOps, BooleanOpsScalar};
use geoarrow::trait_::ArrayAccessor;
use geoarrow::ArrayBase;

use crate::data_types::{
    any_single_geometry_type_input, any_two_geometry_type_input, parse_to_geometry_array,
    parse_to_native_array, GEOMETRY_TYPE,
};
use crate::error::GeoDataFusionResult;

#[derive(Debug)]
pub(super) struct Contains {
    signature: Signature,
}

impl Contains {
    pub fn new() -> Self {
        Self {
            signature: any_two_geometry_type_input(),
        }
    }
}

static CONTAINS_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Contains {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_contains"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> datafusion::error::Result<ColumnarValue> {
        Ok(contains_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(CONTAINS_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns TRUE if geometry A contains geometry B.",
                "ST_Contains(geometry)",
            )
            .with_argument("geomA", "geometry")
            .with_argument("geomB", "geometry")
            .build()
        }))
    }
}

fn contains_impl(args: &[ColumnarValue]) -> GeoDataFusionResult<ColumnarValue> {
    let left = ColumnarValue::values_to_arrays(&args[0..1])?
        .into_iter()
        .next()
        .unwrap();

    let left = parse_to_geometry_array(left)?;

    let out = match &args[1] {
        ColumnarValue::Array(arr) => {
            let right = parse_to_geometry_array(arr.clone())?;
            BooleanOps::contains(&left, &right)?
        }
        ColumnarValue::Scalar(scalar) => {
            let right = parse_to_geometry_array(scalar.to_array()?)?;
            let right = right.value(0);
            BooleanOpsScalar::contains(&left, &right)?
        }
    };
    Ok(ColumnarValue::Array(Arc::new(out)))
}
