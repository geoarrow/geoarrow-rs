use std::sync::Arc;

use datafusion::logical_expr::{ColumnarValue, ScalarUDF, Volatility};
use datafusion::prelude::create_udf;
use geoarrow::algorithm::geo::ConvexHull;
use geoarrow::array::{CoordType, GeometryArray};
use geoarrow::datatypes::NativeType;
use geoarrow::ArrayBase;

use crate::error::GeoDataFusionResult;
use crate::udf::geo::util::{geometry_data_type, parse_single_arg_to_geometry_array};

/// ST_ConvexHull
///
/// - The convex hull of a geometry represents the minimum convex geometry that encloses all
///   geometries within the set.
pub fn convex_hull() -> ScalarUDF {
    create_udf(
        "st_convex_hull",
        vec![NativeType::Geometry(CoordType::Separated).to_data_type()],
        geometry_data_type(),
        Volatility::Immutable,
        Arc::new(|args: &[ColumnarValue]| Ok(_convex_hull(args)?)),
    )
}

fn _convex_hull(args: &[ColumnarValue]) -> GeoDataFusionResult<ColumnarValue> {
    let geom_arr = parse_single_arg_to_geometry_array(args)?;
    let output = geom_arr.convex_hull().into_coord_type(CoordType::Separated);
    Ok(GeometryArray::from(output).into_array_ref().into())
}
