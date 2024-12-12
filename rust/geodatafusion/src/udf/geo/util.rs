use arrow_schema::DataType;
use datafusion::logical_expr::ColumnarValue;
use geoarrow::array::{CoordType, GeometryArray};
use geoarrow::datatypes::{Dimension, NativeType};

use crate::error::GeoDataFusionResult;

pub(crate) fn parse_single_arg_to_geometry_array(
    args: &[ColumnarValue],
) -> GeoDataFusionResult<GeometryArray> {
    let args = ColumnarValue::values_to_arrays(args)?;
    let arg = args.into_iter().next().unwrap();
    Ok(GeometryArray::try_from(arg.as_ref())?)
}

pub(crate) fn box2d_data_type() -> DataType {
    NativeType::Rect(Dimension::XY).to_data_type()
}

pub(crate) fn box3d_data_type() -> DataType {
    NativeType::Rect(Dimension::XYZ).to_data_type()
}

pub(crate) fn geometry_data_type() -> DataType {
    NativeType::Geometry(CoordType::Separated).to_data_type()
}
