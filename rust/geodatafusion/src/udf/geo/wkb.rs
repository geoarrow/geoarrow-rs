use std::sync::Arc;

use arrow::array::AsArray;
use arrow_schema::DataType;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{ColumnarValue, ScalarUDF, Volatility};
use datafusion::prelude::create_udf;
use geoarrow::array::{AsNativeArray, CoordType, GeometryArray, WKBArray};
use geoarrow::datatypes::NativeType;
use geoarrow::io::wkb::to_wkb;
use geoarrow::{ArrayBase, NativeArray};

/// Returns the Well-Known Binary representation of the geometry.
///
/// There are 2 variants of the function. The first variant takes no endian encoding paramater and
/// defaults to little endian. The second variant takes a second argument denoting the encoding -
/// using little-endian ('NDR') or big-endian ('XDR') encoding.
pub fn as_binary() -> ScalarUDF {
    let udf = Arc::new(|args: &[ColumnarValue]| {
        let args = ColumnarValue::values_to_arrays(args)?;
        let arg = args.into_iter().next().unwrap();
        let geom_arr = GeometryArray::try_from(arg.as_ref()).unwrap();
        let wkb_arr = to_wkb::<i32>(geom_arr.as_ref());
        Ok(ColumnarValue::from(wkb_arr.into_array_ref()))
    });

    create_udf(
        "st_asbinary",
        vec![NativeType::Geometry(CoordType::Separated).to_data_type()],
        DataType::Binary.into(),
        Volatility::Immutable,
        udf,
    )
}

pub fn from_wkb() -> ScalarUDF {
    let udf = Arc::new(|args: &[ColumnarValue]| {
        let args = ColumnarValue::values_to_arrays(args)?;
        let arg = args.into_iter().next().unwrap();
        let wkb_arr = WKBArray::new(arg.as_binary::<i32>().clone(), Default::default());
        let native_arr = geoarrow::io::wkb::from_wkb(
            &wkb_arr,
            NativeType::Geometry(CoordType::Separated),
            false,
        )
        .map_err(|err| DataFusionError::External(Box::new(err)))?;
        Ok(ColumnarValue::from(
            native_arr.as_ref().as_geometry().to_array_ref(),
        ))
    });

    create_udf(
        "st_geomfromwkb",
        vec![DataType::Binary],
        NativeType::Geometry(CoordType::Separated)
            .to_data_type()
            .into(),
        Volatility::Immutable,
        udf,
    )
}
