use std::sync::Arc;

use arrow_array::ArrayRef;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{Signature, Volatility};
use geoarrow_array::array::{GeometryArray, PointArray, RectArray};
use geoarrow_array::{GeoArrowArray, GeoArrowType};
use geoarrow_schema::{BoxType, CoordType, Dimension, GeometryType, PointType};

use crate::error::GeoDataFusionResult;

#[allow(non_snake_case)]
pub fn POINT2D_TYPE() -> GeoArrowType {
    GeoArrowType::Point(PointType::new(
        CoordType::Separated,
        Dimension::XY,
        Default::default(),
    ))
}

#[allow(non_snake_case)]
pub fn POINT3D_TYPE() -> GeoArrowType {
    GeoArrowType::Point(PointType::new(
        CoordType::Separated,
        Dimension::XYZ,
        Default::default(),
    ))
}

#[allow(non_snake_case)]
pub fn BOX2D_TYPE() -> GeoArrowType {
    GeoArrowType::Rect(BoxType::new(Dimension::XY, Default::default()))
}

#[allow(non_snake_case)]
pub fn BOX3D_TYPE() -> GeoArrowType {
    GeoArrowType::Rect(BoxType::new(Dimension::XYZ, Default::default()))
}

#[allow(non_snake_case)]
pub fn GEOMETRY_TYPE() -> GeoArrowType {
    GeoArrowType::Geometry(GeometryType::new(CoordType::Separated, Default::default()))
}

pub(crate) fn any_single_geometry_type_input() -> Signature {
    Signature::uniform(
        1,
        vec![
            POINT2D_TYPE().into(),
            POINT3D_TYPE().into(),
            BOX2D_TYPE().into(),
            BOX3D_TYPE().into(),
            GEOMETRY_TYPE().into(),
        ],
        Volatility::Immutable,
    )
}

/// This will not cast a PointArray to a GeometryArray
pub(crate) fn parse_to_native_array(
    array: ArrayRef,
) -> GeoDataFusionResult<Arc<dyn GeoArrowArray>> {
    let data_type = array.data_type();
    if data_type.equals_datatype(&POINT2D_TYPE().into()) {
        let point_type = PointType::new(CoordType::Separated, Dimension::XY, Default::default());
        let point_array = PointArray::try_from((array.as_ref(), point_type))?;
        Ok(Arc::new(point_array))
    } else if data_type.equals_datatype(&POINT3D_TYPE().into()) {
        let point_type = PointType::new(CoordType::Separated, Dimension::XYZ, Default::default());
        let point_array = PointArray::try_from((array.as_ref(), point_type))?;
        Ok(Arc::new(point_array))
    } else if data_type.equals_datatype(&BOX2D_TYPE().into()) {
        let rect_type = BoxType::new(Dimension::XY, Default::default());
        let rect_array = RectArray::try_from((array.as_ref(), rect_type))?;
        Ok(Arc::new(rect_array))
    } else if data_type.equals_datatype(&BOX3D_TYPE().into()) {
        let rect_type = BoxType::new(Dimension::XYZ, Default::default());
        let rect_array = RectArray::try_from((array.as_ref(), rect_type))?;
        Ok(Arc::new(rect_array))
    } else if data_type.equals_datatype(&GEOMETRY_TYPE().into()) {
        let geometry_type = GeometryType::new(CoordType::Separated, Default::default());
        Ok(Arc::new(GeometryArray::try_from((
            array.as_ref(),
            geometry_type,
        ))?))
    } else {
        Err(DataFusionError::Execution(format!("Unexpected input data type: {}", data_type)).into())
    }
}
