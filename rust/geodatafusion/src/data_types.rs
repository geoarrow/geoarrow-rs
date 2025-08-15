use arrow_schema::DataType;
use datafusion::logical_expr::{Signature, Volatility};
use geoarrow_schema::{
    BoxType, CoordType, Dimension, GeometryCollectionType, GeometryType, LineStringType,
    MultiLineStringType, MultiPointType, MultiPolygonType, PointType, PolygonType,
};

pub(crate) fn any_geometry_type() -> Vec<DataType> {
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

    // Wkb
    valid_types.push(DataType::Binary);
    valid_types.push(DataType::LargeBinary);
    valid_types.push(DataType::BinaryView);

    // Wkt
    valid_types.push(DataType::Utf8);
    valid_types.push(DataType::LargeUtf8);
    valid_types.push(DataType::Utf8View);

    valid_types
}

pub(crate) fn any_single_geometry_type_input() -> Signature {
    Signature::uniform(1, any_geometry_type(), Volatility::Immutable)
}

pub(crate) fn any_point_type_input(arg_count: usize) -> Signature {
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
        }

        valid_types.push(
            GeometryType::new(Default::default())
                .with_coord_type(coord_type)
                .data_type(),
        );
    }

    Signature::uniform(arg_count, valid_types, Volatility::Immutable)
}
