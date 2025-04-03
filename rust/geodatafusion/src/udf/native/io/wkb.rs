use std::any::Any;
use std::sync::OnceLock;

use arrow::array::AsArray;
use arrow_schema::DataType;
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, Volatility,
};
use geoarrow::array::WKBArray;
use geoarrow::datatypes::NativeType;
use geoarrow::io::wkb::{from_wkb, to_wkb};
use geoarrow::ArrayBase;
use geoarrow_schema::{CoordType, GeometryType};

use crate::data_types::{any_single_geometry_type_input, parse_to_native_array, GEOMETRY_TYPE};
use crate::error::GeoDataFusionResult;

#[derive(Debug)]
pub(super) struct AsBinary {
    signature: Signature,
}

impl AsBinary {
    pub fn new() -> Self {
        // TODO: extend to allow specifying little/big endian
        Self {
            signature: any_single_geometry_type_input(),
        }
    }
}

static AS_BINARY_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for AsBinary {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_asbinary"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Binary)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> datafusion::error::Result<ColumnarValue> {
        Ok(as_binary_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(AS_BINARY_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the OGC/ISO Well-Known Binary (WKB) representation of the geometry.",
                "ST_AsBinary(geometry)",
            )
            .with_argument("g1", "geometry")
            .build()
        }))
    }
}

fn as_binary_impl(args: &[ColumnarValue]) -> GeoDataFusionResult<ColumnarValue> {
    let array = ColumnarValue::values_to_arrays(args)?
        .into_iter()
        .next()
        .unwrap();
    let native_array = parse_to_native_array(array)?;
    let wkb_arr = to_wkb::<i32>(native_array.as_ref());
    Ok(wkb_arr.into_array_ref().into())
}

#[derive(Debug)]
pub(super) struct GeomFromWKB {
    signature: Signature,
}

impl GeomFromWKB {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Binary], Volatility::Immutable),
        }
    }
}

static GEOM_FROM_WKB_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for GeomFromWKB {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_geomfromwkb"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(GEOMETRY_TYPE().into())
    }

    fn invoke(&self, args: &[ColumnarValue]) -> datafusion::error::Result<ColumnarValue> {
        Ok(geom_from_wkb_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(GEOM_FROM_WKB_DOC.get_or_init(|| {
            Documentation::builder(DOC_SECTION_OTHER, "Takes a well-known binary representation of a geometry and a Spatial Reference System ID (SRID) and creates an instance of the appropriate geometry type", "ST_GeomFromWKB(buffer)")
                .with_argument("geom", "WKB buffers")
                .build()
        }))
    }
}

fn geom_from_wkb_impl(args: &[ColumnarValue]) -> GeoDataFusionResult<ColumnarValue> {
    let array = ColumnarValue::values_to_arrays(args)?
        .into_iter()
        .next()
        .unwrap();
    let wkb_arr = WKBArray::new(array.as_binary::<i32>().clone(), Default::default());
    let native_arr = from_wkb(
        &wkb_arr,
        NativeType::Geometry(GeometryType::new(CoordType::Separated, Default::default())),
        false,
    )?;
    Ok(native_arr.to_array_ref().into())
}
