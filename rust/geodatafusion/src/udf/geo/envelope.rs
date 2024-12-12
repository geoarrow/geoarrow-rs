use std::sync::Arc;

use datafusion::logical_expr::{ColumnarValue, ScalarUDF, Volatility};
use datafusion::prelude::create_udf;
use geoarrow::algorithm::native::BoundingRectArray;
use geoarrow::array::PolygonArray;
use geoarrow::ArrayBase;

use crate::error::GeoDataFusionResult;
use crate::udf::geo::util::{geometry_data_type, parse_single_arg_to_geometry_array};

/// ST_Envelope
///
/// - Returns a geometry representing the double precision (float64) bounding box of the supplied geometry.
pub fn envelope() -> ScalarUDF {
    create_udf(
        "st_envelope",
        vec![geometry_data_type()],
        geometry_data_type(),
        Volatility::Immutable,
        Arc::new(|args: &[ColumnarValue]| Ok(_envelope(args)?)),
    )
}

fn _envelope(args: &[ColumnarValue]) -> GeoDataFusionResult<ColumnarValue> {
    let geom_arr = parse_single_arg_to_geometry_array(args)?;
    let rect_arr = geom_arr.bounding_rect();
    let polygon_arr = PolygonArray::from(rect_arr);
    Ok(polygon_arr.into_array_ref().into())
}
