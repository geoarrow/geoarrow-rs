use std::any::Any;
use std::sync::OnceLock;

use arrow_array::cast::AsArray;
use arrow_schema::DataType;
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, Volatility,
};
use geoarrow::ArrayBase;
use geoarrow::array::WKTArray;
use geoarrow::io::wkt::{ToWKT, read_wkt};
use geoarrow_schema::CoordType;

use crate::data_types::{GEOMETRY_TYPE, any_single_geometry_type_input, parse_to_native_array};
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
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the OGC Well-Known Text (WKT) representation of the geometry/geography.",
                "ST_AsText(geometry)",
            )
            .with_argument("g1", "geometry")
            .build()
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
        Self {
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        }
    }
}

static GEOM_FROM_TEXT_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for GeomFromText {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_geomfromtext"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(GEOMETRY_TYPE().into())
    }

    fn invoke(&self, args: &[ColumnarValue]) -> datafusion::error::Result<ColumnarValue> {
        Ok(geom_from_text_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(GEOM_FROM_TEXT_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Constructs a geometry object from the OGC Well-Known text representation.",
                "ST_GeomFromText(text)",
            )
            .with_argument("g1", "geometry")
            .build()
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

#[cfg(test)]
mod test {
    use datafusion::prelude::*;

    use crate::udf::native::register_native;

    #[ignore = "Union fields length must match child arrays length"]
    #[tokio::test]
    async fn test() {
        let ctx = SessionContext::new();
        register_native(&ctx);

        let out = ctx.sql("SELECT ST_GeomFromText('LINESTRING(-71.160281 42.258729,-71.160837 42.259113,-71.161144 42.25932)');").await.unwrap();
        out.show().await.unwrap();
    }
}
