use std::sync::Arc;

use arrow::array::AsArray;
use arrow_schema::DataType;
use datafusion::logical_expr::{ColumnarValue, ScalarUDF, Volatility};
use datafusion::prelude::create_udf;
use geoarrow::array::{AsNativeArray, CoordType, WKTArray};
use geoarrow::io::wkt::ToWKT;
use geoarrow::{ArrayBase, NativeArray};

use crate::error::GeoDataFusionResult;
use crate::udf::geo::util::{geometry_data_type, parse_single_arg_to_geometry_array};

/// ST_AsText
///
/// - Return the Well-Known Text (WKT) representation of the geometry/geography without SRID metadata.
pub fn as_text() -> ScalarUDF {
    create_udf(
        "st_astext",
        vec![geometry_data_type()],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(|args: &[ColumnarValue]| Ok(_as_text(args)?)),
    )
}

fn _as_text(args: &[ColumnarValue]) -> GeoDataFusionResult<ColumnarValue> {
    let geom_arr = parse_single_arg_to_geometry_array(args)?;
    let wkt_arr = geom_arr.as_ref().to_wkt::<i32>()?;
    Ok(wkt_arr.into_array_ref().into())
}

/// ST_GeomFromText
///
/// - Return a specified ST_Geometry value from Well-Known Text representation (WKT).
pub fn from_text() -> ScalarUDF {
    create_udf(
        "st_geomfromtext",
        vec![DataType::Utf8],
        geometry_data_type(),
        Volatility::Immutable,
        Arc::new(|args: &[ColumnarValue]| Ok(_from_text(args)?)),
    )
}

fn _from_text(args: &[ColumnarValue]) -> GeoDataFusionResult<ColumnarValue> {
    let arg = ColumnarValue::values_to_arrays(args)?
        .into_iter()
        .next()
        .unwrap();
    let wkt_arr = WKTArray::new(arg.as_string::<i32>().clone(), Default::default());
    let native_arr = geoarrow::io::wkt::read_wkt(&wkt_arr, CoordType::Separated, false)?;
    Ok(native_arr.as_ref().as_geometry().to_array_ref().into())
}
