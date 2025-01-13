use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_schema::{DataType, Fields};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{Signature, Volatility};
use geoarrow::array::{CoordType, GeometryArray, NativeArrayDyn, PointArray, RectArray};
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
            struct_wrapped_type(POINT2D_TYPE),
        ],
        Volatility::Immutable,
    )
}

/// This will not cast a PointArray to a GeometryArray
pub(crate) fn parse_to_native_array(array: ArrayRef) -> GeoDataFusionResult<Arc<dyn NativeArray>> {
    let data_type = array.data_type();

    match data_type {
        DataType::Struct(fields) => {
            if fields.size() != 1 {
                return Err(DataFusionError::Execution(
                    "Extension workaround struct with fields.size() != 1".to_string(),
                )
                .into());
            }

            let native_array_dyn = NativeArrayDyn::from_arrow_array(&array, &fields[0])?;
            return Ok(native_array_dyn.into());
        }
        _ => {}
    }

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

fn struct_wrapped_type(native_type: NativeType) -> DataType {
    return DataType::Struct(Fields::from(vec![
        native_type.to_field(native_type.extension_name(), false)
    ]));
}
