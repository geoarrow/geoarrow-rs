use std::any::Any;
use std::sync::{Arc, OnceLock};

use arrow::array::UInt8Builder;
use arrow_schema::DataType;
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{ColumnarValue, Documentation, ScalarUDFImpl, Signature};
use datafusion::scalar::ScalarValue;
use geo_traits::{GeometryTrait, PointTrait};
use geoarrow::array::AsNativeArray;
use geoarrow::datatypes::NativeType;
use geoarrow::trait_::ArrayAccessor;

use crate::data_types::{any_single_geometry_type_input, parse_to_native_array};
use crate::error::GeoDataFusionResult;

#[derive(Debug)]
pub(super) struct CoordDim {
    signature: Signature,
}

impl CoordDim {
    pub fn new() -> Self {
        Self {
            signature: any_single_geometry_type_input(),
        }
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for CoordDim {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_coorddim"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::UInt8)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> datafusion::error::Result<ColumnarValue> {
        Ok(coord_dim_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Return the coordinate dimension of the ST_Geometry value.",
                "ST_CoordDim(geometry)",
            )
            .with_argument("g1", "geometry")
            .build()
        }))
    }
}

fn coord_dim_impl(args: &[ColumnarValue]) -> GeoDataFusionResult<ColumnarValue> {
    let array = ColumnarValue::values_to_arrays(args)?
        .into_iter()
        .next()
        .unwrap();
    let native_array = parse_to_native_array(array)?;

    match native_array.data_type() {
        NativeType::Point(_) => {
            let array_ref = native_array.as_ref();
            let arr = array_ref.as_point();
            let mut output_array = UInt8Builder::with_capacity(native_array.len());
            for geom in arr.iter() {
                output_array.append_option(geom.map(|g| g.dim().size().try_into().unwrap()));
            }
            Ok(ColumnarValue::Array(Arc::new(output_array.finish())))
        }
        NativeType::Rect(t) => Ok(ColumnarValue::Scalar(ScalarValue::UInt8(Some(
            t.dimension().size().try_into().unwrap(),
        )))),
        NativeType::Geometry(_) => {
            let array_ref = native_array.as_ref();
            let arr = array_ref.as_geometry();
            let mut output_array = UInt8Builder::with_capacity(native_array.len());
            for geom in arr.iter() {
                output_array.append_option(geom.map(|g| g.dim().size().try_into().unwrap()));
            }
            Ok(ColumnarValue::Array(Arc::new(output_array.finish())))
        }
        _ => unreachable!(),
    }
}
