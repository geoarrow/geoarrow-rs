use std::sync::Arc;

use arrow_array::ArrayRef;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{Signature, Volatility};
use geoarrow::array::{CoordType, GeometryArray, PointArray, RectArray};
use geoarrow::datatypes::{Dimension, NativeType};
use geoarrow::NativeArray;

use crate::error::GeoDataFusionResult;

pub const POINT2D_TYPE: NativeType = NativeType::Point(CoordType::Separated, Dimension::XY);
pub const POINT3D_TYPE: NativeType = NativeType::Point(CoordType::Separated, Dimension::XYZ);
pub const BOX2D_TYPE: NativeType = NativeType::Rect(Dimension::XY);
pub const BOX3D_TYPE: NativeType = NativeType::Rect(Dimension::XYZ);
pub const GEOMETRY_TYPE: NativeType = NativeType::Geometry(CoordType::Separated);

pub(crate) fn any_single_geometry_type_input() -> Signature {
    Signature::uniform(
        1,
        vec![
            POINT2D_TYPE.into(),
            POINT3D_TYPE.into(),
            BOX2D_TYPE.into(),
            BOX3D_TYPE.into(),
            GEOMETRY_TYPE.into(),
        ],
        Volatility::Immutable,
    )
}

/// This will not cast a PointArray to a GeometryArray
pub(crate) fn parse_to_native_array(array: ArrayRef) -> GeoDataFusionResult<Arc<dyn NativeArray>> {
    let data_type = array.data_type();
    if data_type.equals_datatype(&POINT2D_TYPE.into()) {
        let point_array = PointArray::try_from((array.as_ref(), Dimension::XY))?;
        Ok(Arc::new(point_array))
    } else if data_type.equals_datatype(&POINT3D_TYPE.into()) {
        let point_array = PointArray::try_from((array.as_ref(), Dimension::XYZ))?;
        Ok(Arc::new(point_array))
    } else if data_type.equals_datatype(&BOX2D_TYPE.into()) {
        let rect_array = RectArray::try_from((array.as_ref(), Dimension::XY))?;
        Ok(Arc::new(rect_array))
    } else if data_type.equals_datatype(&BOX3D_TYPE.into()) {
        let rect_array = RectArray::try_from((array.as_ref(), Dimension::XYZ))?;
        Ok(Arc::new(rect_array))
    } else if data_type.equals_datatype(&GEOMETRY_TYPE.into()) {
        Ok(Arc::new(GeometryArray::try_from(array.as_ref())?))
    } else {
        Err(DataFusionError::Execution(format!("Unexpected input data type: {}", data_type)).into())
    }
}
