use std::sync::Arc;

use arrow_array::ArrayRef;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{Signature, Volatility};
use geoarrow_array::GeoArrowArray;
use geoarrow_array::array::{GeometryArray, PointArray, RectArray};
use geoarrow_schema::{
    BoxType, CoordType, Dimension, GeoArrowType, GeometryCollectionType, GeometryType,
    LineStringType, MultiLineStringType, MultiPointType, MultiPolygonType, PointType, PolygonType,
};

use crate::error::GeoDataFusionResult;

#[allow(non_snake_case)]
pub(crate) fn POINT2D_TYPE() -> GeoArrowType {
    GeoArrowType::Point(
        PointType::new(Dimension::XY, Default::default()).with_coord_type(CoordType::Separated),
    )
}

#[allow(non_snake_case)]
pub(crate) fn POINT3D_TYPE() -> GeoArrowType {
    GeoArrowType::Point(
        PointType::new(Dimension::XYZ, Default::default()).with_coord_type(CoordType::Separated),
    )
}

#[allow(non_snake_case)]
pub(crate) fn BOX2D_TYPE() -> GeoArrowType {
    GeoArrowType::Rect(BoxType::new(Dimension::XY, Default::default()))
}

#[allow(non_snake_case)]
pub(crate) fn BOX3D_TYPE() -> GeoArrowType {
    GeoArrowType::Rect(BoxType::new(Dimension::XYZ, Default::default()))
}

#[allow(non_snake_case)]
pub(crate) fn GEOMETRY_TYPE() -> GeoArrowType {
    GeoArrowType::Geometry(
        GeometryType::new(Default::default()).with_coord_type(CoordType::Separated),
    )
}

pub(crate) fn any_single_geometry_type_input() -> Signature {
    let mut valid_types = vec![];

    for coord_type in [CoordType::Separated, CoordType::Interleaved] {
        for dim in [
            Dimension::XY,
            Dimension::XYZ,
            Dimension::XYM,
            Dimension::XYZM,
        ] {
            valid_types.push(
                PointType::new(dim, Default::default())
                    .with_coord_type(coord_type)
                    .data_type(),
            );
            valid_types.push(
                LineStringType::new(dim, Default::default())
                    .with_coord_type(coord_type)
                    .data_type(),
            );
            valid_types.push(
                PolygonType::new(dim, Default::default())
                    .with_coord_type(coord_type)
                    .data_type(),
            );
            valid_types.push(
                MultiPointType::new(dim, Default::default())
                    .with_coord_type(coord_type)
                    .data_type(),
            );
            valid_types.push(
                MultiLineStringType::new(dim, Default::default())
                    .with_coord_type(coord_type)
                    .data_type(),
            );
            valid_types.push(
                MultiPolygonType::new(dim, Default::default())
                    .with_coord_type(coord_type)
                    .data_type(),
            );
            valid_types.push(
                GeometryCollectionType::new(dim, Default::default())
                    .with_coord_type(coord_type)
                    .data_type(),
            );
        }
    }

    for coord_type in [CoordType::Separated, CoordType::Interleaved] {
        valid_types.push(
            GeometryType::new(Default::default())
                .with_coord_type(coord_type)
                .data_type(),
        );
    }

    for dim in [
        Dimension::XY,
        Dimension::XYZ,
        Dimension::XYM,
        Dimension::XYZM,
    ] {
        valid_types.push(BoxType::new(dim, Default::default()).data_type());
    }

    Signature::uniform(1, valid_types, Volatility::Immutable)
}

/// This will not cast a PointArray to a GeometryArray
pub(crate) fn parse_to_native_array(
    array: ArrayRef,
) -> GeoDataFusionResult<Arc<dyn GeoArrowArray>> {
    let data_type = array.data_type();
    if data_type.equals_datatype(&POINT2D_TYPE().into()) {
        let point_type =
            PointType::new(Dimension::XY, Default::default()).with_coord_type(CoordType::Separated);
        let point_array = PointArray::try_from((array.as_ref(), point_type))?;
        Ok(Arc::new(point_array))
    } else if data_type.equals_datatype(&POINT3D_TYPE().into()) {
        let point_type = PointType::new(Dimension::XYZ, Default::default())
            .with_coord_type(CoordType::Separated);
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
        let geometry_type =
            GeometryType::new(Default::default()).with_coord_type(CoordType::Separated);
        Ok(Arc::new(GeometryArray::try_from((
            array.as_ref(),
            geometry_type,
        ))?))
    } else {
        Err(DataFusionError::Execution(format!("Unexpected input data type: {}", data_type)).into())
    }
}
