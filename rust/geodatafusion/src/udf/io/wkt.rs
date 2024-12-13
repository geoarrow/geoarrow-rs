use std::any::Any;
use std::sync::OnceLock;

use arrow::array::AsArray;
use arrow_schema::DataType;
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, Volatility,
};
use geoarrow::array::{CoordType, WKTArray};
use geoarrow::io::wkt::{read_wkt, ToWKT};
use geoarrow::ArrayBase;

use crate::data_types::{any_single_geometry_type_input, parse_to_native_array, GEOMETRY_TYPE};
use crate::error::GeoDataFusionResult;

#[derive(Debug)]
pub(super) struct AsText {
    signature: Signature,
}

impl AsText {
    pub fn new() -> Self {
        // TODO: extend to allow specifying little/big endian
        Self {
            signature: any_single_geometry_type_input(),
        }
    }
}

static AS_TEXT_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for AsText {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_astext"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> datafusion::error::Result<ColumnarValue> {
        Ok(as_text_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(AS_TEXT_DOC.get_or_init(|| {
            Documentation::builder()
                .with_doc_section(DOC_SECTION_OTHER)
                .with_description(
                    "Returns the OGC Well-Known Text (WKT) representation of the geometry/geography.",
                )
                .with_argument("g1", "geometry")
                .build()
                .unwrap()
        }))
    }
}

fn as_text_impl(args: &[ColumnarValue]) -> GeoDataFusionResult<ColumnarValue> {
    let array = ColumnarValue::values_to_arrays(args)?
        .into_iter()
        .next()
        .unwrap();
    let native_array = parse_to_native_array(array)?;
    let wkt_arr = native_array.as_ref().to_wkt::<i32>()?;
    Ok(wkt_arr.into_array_ref().into())
}

#[derive(Debug)]
pub(super) struct GeomFromText {
    signature: Signature,
}

impl GeomFromText {
    pub fn new() -> Self {
        // TODO: extend to allow specifying little/big endian
        Self {
            signature: Signature::coercible(vec![DataType::Utf8], Volatility::Immutable),
        }
    }
}

static GEOM_FROM_TEXT_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for GeomFromText {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_astext"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(GEOMETRY_TYPE.into())
    }

    fn invoke(&self, args: &[ColumnarValue]) -> datafusion::error::Result<ColumnarValue> {
        Ok(geom_from_text_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(GEOM_FROM_TEXT_DOC.get_or_init(|| {
            Documentation::builder()
                .with_doc_section(DOC_SECTION_OTHER)
                .with_description(
                    "Constructs a geometry object from the OGC Well-Known text representation.",
                )
                .with_argument("g1", "geometry")
                .build()
                .unwrap()
        }))
    }
}

fn geom_from_text_impl(args: &[ColumnarValue]) -> GeoDataFusionResult<ColumnarValue> {
    let array = ColumnarValue::values_to_arrays(args)?
        .into_iter()
        .next()
        .unwrap();
    let wkt_arr = WKTArray::new(array.as_string::<i32>().clone(), Default::default());
    let native_arr = read_wkt(&wkt_arr, CoordType::Separated, false)?;
    Ok(native_arr.to_array_ref().into())
}
