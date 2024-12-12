use std::sync::Arc;

use arrow::array::UInt8Builder;
use arrow_array::ArrayRef;
use arrow_schema::DataType;
use datafusion::logical_expr::{ColumnarValue, ScalarUDF, Volatility};
use datafusion::prelude::create_udf;
use geo_traits::GeometryTrait;
use geoarrow::trait_::ArrayAccessor;
use geoarrow::ArrayBase;

use crate::error::GeoDataFusionResult;
use crate::udf::geo::util::{geometry_data_type, parse_single_arg_to_geometry_array};

/// ST_CoordDim
///
/// - Return the coordinate dimension of the ST_Geometry value.
pub fn coord_dim() -> ScalarUDF {
    create_udf(
        "st_coord_dim",
        vec![geometry_data_type()],
        DataType::UInt8,
        Volatility::Immutable,
        Arc::new(|args: &[ColumnarValue]| Ok(_coord_dim(args)?)),
    )
}

fn _coord_dim(args: &[ColumnarValue]) -> GeoDataFusionResult<ColumnarValue> {
    let geom_arr = parse_single_arg_to_geometry_array(args)?;
    let mut output_array = UInt8Builder::with_capacity(geom_arr.len());

    for geom in geom_arr.iter() {
        output_array.append_option(geom.map(|g| g.dim().size().try_into().unwrap()));
    }

    Ok(ColumnarValue::from(
        Arc::new(output_array.finish()) as ArrayRef
    ))
}
