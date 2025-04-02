use std::collections::HashSet;

use arrow_schema::extension::ExtensionType;
use arrow_schema::{ArrowError, DataType};

use crate::metadata::Metadata;
use crate::{CoordType, Dimension};

pub struct PointType {
    coord_type: CoordType,
    dim: Dimension,
    metadata: Metadata,
}

impl ExtensionType for PointType {
    const NAME: &'static str = "geoarrow.point";

    type Metadata = Metadata;

    fn metadata(&self) -> &Self::Metadata {
        &self.metadata
    }

    fn serialize_metadata(&self) -> Option<String> {
        self.metadata.serialize()
    }

    fn deserialize_metadata(metadata: Option<&str>) -> Result<Self::Metadata, ArrowError> {
        Metadata::deserialize(metadata)
    }

    fn supports_data_type(&self, data_type: &DataType) -> Result<(), ArrowError> {
        let (coord_type, dim) = parse_point_data_type(data_type)?;
        if coord_type != self.coord_type {
            return Err(ArrowError::SchemaError(format!(
                "Expected coordinate type {:?}, but got {:?}",
                self.coord_type, coord_type
            )));
        }
        if dim != self.dim {
            return Err(ArrowError::SchemaError(format!(
                "Expected dimension {:?}, but got {:?}",
                self.dim, dim
            )));
        }
        Ok(())
    }

    fn try_new(data_type: &DataType, metadata: Self::Metadata) -> Result<Self, ArrowError> {
        let (coord_type, dim) = parse_point_data_type(data_type)?;
        Ok(Self {
            coord_type,
            dim,
            metadata,
        })
    }
}

fn parse_point_data_type(data_type: &DataType) -> Result<(CoordType, Dimension), ArrowError> {
    match data_type {
        DataType::FixedSizeList(inner_field, _list_size) => Ok((
            CoordType::Interleaved,
            Dimension::from_interleaved_field(inner_field),
        )),
        DataType::Struct(struct_fields) => Ok((
            CoordType::Separated,
            Dimension::from_separated_field(struct_fields),
        )),
        dt => Err(ArrowError::SchemaError(format!(
            "Unexpected data type {dt}"
        ))),
    }
}

pub struct LineStringType {
    coord_type: CoordType,
    dim: Dimension,
    metadata: Metadata,
}

impl ExtensionType for LineStringType {
    const NAME: &'static str = "geoarrow.linestring";

    type Metadata = Metadata;

    fn metadata(&self) -> &Self::Metadata {
        &self.metadata
    }

    fn serialize_metadata(&self) -> Option<String> {
        self.metadata.serialize()
    }

    fn deserialize_metadata(metadata: Option<&str>) -> Result<Self::Metadata, ArrowError> {
        Metadata::deserialize(metadata)
    }

    fn supports_data_type(&self, data_type: &DataType) -> Result<(), ArrowError> {
        let (coord_type, dim) = parse_linestring(data_type)?;
        if coord_type != self.coord_type {
            return Err(ArrowError::SchemaError(format!(
                "Expected coordinate type {:?}, but got {:?}",
                self.coord_type, coord_type
            )));
        }
        if dim != self.dim {
            return Err(ArrowError::SchemaError(format!(
                "Expected dimension {:?}, but got {:?}",
                self.dim, dim
            )));
        }
        Ok(())
    }

    fn try_new(data_type: &DataType, metadata: Self::Metadata) -> Result<Self, ArrowError> {
        let (coord_type, dim) = parse_linestring(data_type)?;
        Ok(Self {
            coord_type,
            dim,
            metadata,
        })
    }
}

fn parse_linestring(data_type: &DataType) -> Result<(CoordType, Dimension), ArrowError> {
    match data_type {
        DataType::List(inner_field) | DataType::LargeList(inner_field) => {
            parse_point_data_type(inner_field.data_type())
        }
        dt => Err(ArrowError::SchemaError(format!(
            "Unexpected data type {dt}"
        ))),
    }
}

pub struct PolygonType {
    coord_type: CoordType,
    dim: Dimension,
    metadata: Metadata,
}

impl ExtensionType for PolygonType {
    const NAME: &'static str = "geoarrow.polygon";

    type Metadata = Metadata;

    fn metadata(&self) -> &Self::Metadata {
        &self.metadata
    }

    fn serialize_metadata(&self) -> Option<String> {
        self.metadata.serialize()
    }

    fn deserialize_metadata(metadata: Option<&str>) -> Result<Self::Metadata, ArrowError> {
        Metadata::deserialize(metadata)
    }

    fn supports_data_type(&self, data_type: &DataType) -> Result<(), ArrowError> {
        let (coord_type, dim) = parse_polygon(data_type)?;
        if coord_type != self.coord_type {
            return Err(ArrowError::SchemaError(format!(
                "Expected coordinate type {:?}, but got {:?}",
                self.coord_type, coord_type
            )));
        }
        if dim != self.dim {
            return Err(ArrowError::SchemaError(format!(
                "Expected dimension {:?}, but got {:?}",
                self.dim, dim
            )));
        }
        Ok(())
    }

    fn try_new(data_type: &DataType, metadata: Self::Metadata) -> Result<Self, ArrowError> {
        let (coord_type, dim) = parse_polygon(data_type)?;
        Ok(Self {
            coord_type,
            dim,
            metadata,
        })
    }
}

fn parse_polygon(data_type: &DataType) -> Result<(CoordType, Dimension), ArrowError> {
    match data_type {
        DataType::List(inner1) => match inner1.data_type() {
            DataType::List(inner2) => parse_point_data_type(inner2.data_type()),
            _ => panic!(),
        },
        DataType::LargeList(inner1) => match inner1.data_type() {
            DataType::LargeList(inner2) => parse_point_data_type(inner2.data_type()),
            _ => panic!(),
        },
        dt => Err(ArrowError::SchemaError(format!(
            "Unexpected data type {dt}"
        ))),
    }
}

pub struct MultiPointType {
    coord_type: CoordType,
    dim: Dimension,
    metadata: Metadata,
}

impl ExtensionType for MultiPointType {
    const NAME: &'static str = "geoarrow.multipoint";

    type Metadata = Metadata;

    fn metadata(&self) -> &Self::Metadata {
        &self.metadata
    }

    fn serialize_metadata(&self) -> Option<String> {
        self.metadata.serialize()
    }

    fn deserialize_metadata(metadata: Option<&str>) -> Result<Self::Metadata, ArrowError> {
        Metadata::deserialize(metadata)
    }

    fn supports_data_type(&self, data_type: &DataType) -> Result<(), ArrowError> {
        let (coord_type, dim) = parse_multipoint_data_type(data_type)?;
        if coord_type != self.coord_type {
            return Err(ArrowError::SchemaError(format!(
                "Expected coordinate type {:?}, but got {:?}",
                self.coord_type, coord_type
            )));
        }
        if dim != self.dim {
            return Err(ArrowError::SchemaError(format!(
                "Expected dimension {:?}, but got {:?}",
                self.dim, dim
            )));
        }
        Ok(())
    }

    fn try_new(data_type: &DataType, metadata: Self::Metadata) -> Result<Self, ArrowError> {
        let (coord_type, dim) = parse_multipoint_data_type(data_type)?;
        Ok(Self {
            coord_type,
            dim,
            metadata,
        })
    }
}

fn parse_multipoint_data_type(data_type: &DataType) -> Result<(CoordType, Dimension), ArrowError> {
    match data_type {
        DataType::List(inner_field) => parse_point_data_type(inner_field.data_type()),
        DataType::LargeList(inner_field) => parse_point_data_type(inner_field.data_type()),
        dt => Err(ArrowError::SchemaError(format!(
            "Unexpected data type {dt}"
        ))),
    }
}

pub struct MultiLineStringType {
    coord_type: CoordType,
    dim: Dimension,
    metadata: Metadata,
}

impl ExtensionType for MultiLineStringType {
    const NAME: &'static str = "geoarrow.multilinestring";

    type Metadata = Metadata;

    fn metadata(&self) -> &Self::Metadata {
        &self.metadata
    }

    fn serialize_metadata(&self) -> Option<String> {
        self.metadata.serialize()
    }

    fn deserialize_metadata(metadata: Option<&str>) -> Result<Self::Metadata, ArrowError> {
        Metadata::deserialize(metadata)
    }

    fn supports_data_type(&self, data_type: &DataType) -> Result<(), ArrowError> {
        let (coord_type, dim) = parse_multilinestring(data_type)?;
        if coord_type != self.coord_type {
            return Err(ArrowError::SchemaError(format!(
                "Expected coordinate type {:?}, but got {:?}",
                self.coord_type, coord_type
            )));
        }
        if dim != self.dim {
            return Err(ArrowError::SchemaError(format!(
                "Expected dimension {:?}, but got {:?}",
                self.dim, dim
            )));
        }
        Ok(())
    }

    fn try_new(data_type: &DataType, metadata: Self::Metadata) -> Result<Self, ArrowError> {
        let (coord_type, dim) = parse_multilinestring(data_type)?;
        Ok(Self {
            coord_type,
            dim,
            metadata,
        })
    }
}

fn parse_multilinestring(data_type: &DataType) -> Result<(CoordType, Dimension), ArrowError> {
    match data_type {
        DataType::List(inner1) => match inner1.data_type() {
            DataType::List(inner2) => parse_point_data_type(inner2.data_type()),
            _ => panic!(),
        },
        DataType::LargeList(inner1) => match inner1.data_type() {
            DataType::LargeList(inner2) => parse_point_data_type(inner2.data_type()),
            _ => panic!(),
        },
        dt => Err(ArrowError::SchemaError(format!(
            "Unexpected data type {dt}"
        ))),
    }
}

pub struct MultiPolygonType {
    coord_type: CoordType,
    dim: Dimension,
    metadata: Metadata,
}

impl ExtensionType for MultiPolygonType {
    const NAME: &'static str = "geoarrow.multipolygon";

    type Metadata = Metadata;

    fn metadata(&self) -> &Self::Metadata {
        &self.metadata
    }

    fn serialize_metadata(&self) -> Option<String> {
        self.metadata.serialize()
    }

    fn deserialize_metadata(metadata: Option<&str>) -> Result<Self::Metadata, ArrowError> {
        Metadata::deserialize(metadata)
    }

    fn supports_data_type(&self, data_type: &DataType) -> Result<(), ArrowError> {
        let (coord_type, dim) = parse_multipolygon(data_type)?;
        if coord_type != self.coord_type {
            return Err(ArrowError::SchemaError(format!(
                "Expected coordinate type {:?}, but got {:?}",
                self.coord_type, coord_type
            )));
        }
        if dim != self.dim {
            return Err(ArrowError::SchemaError(format!(
                "Expected dimension {:?}, but got {:?}",
                self.dim, dim
            )));
        }
        Ok(())
    }

    fn try_new(data_type: &DataType, metadata: Self::Metadata) -> Result<Self, ArrowError> {
        let (coord_type, dim) = parse_multipolygon(data_type)?;
        Ok(Self {
            coord_type,
            dim,
            metadata,
        })
    }
}

fn parse_multipolygon(data_type: &DataType) -> Result<(CoordType, Dimension), ArrowError> {
    match data_type {
        DataType::List(inner1) => match inner1.data_type() {
            DataType::List(inner2) => match inner2.data_type() {
                DataType::List(inner3) => parse_point_data_type(inner3.data_type()),
                _ => panic!(),
            },
            _ => panic!(),
        },
        DataType::LargeList(inner1) => match inner1.data_type() {
            DataType::LargeList(inner2) => match inner2.data_type() {
                DataType::LargeList(inner3) => parse_point_data_type(inner3.data_type()),
                _ => panic!(),
            },
            _ => panic!(),
        },
        dt => Err(ArrowError::SchemaError(format!(
            "Unexpected data type {dt}"
        ))),
    }
}

pub struct GeometryCollectionType {
    coord_type: CoordType,
    dim: Dimension,
    metadata: Metadata,
}

impl ExtensionType for GeometryCollectionType {
    const NAME: &'static str = "geoarrow.geometrycollection";

    type Metadata = Metadata;

    fn metadata(&self) -> &Self::Metadata {
        &self.metadata
    }

    fn serialize_metadata(&self) -> Option<String> {
        self.metadata.serialize()
    }

    fn deserialize_metadata(metadata: Option<&str>) -> Result<Self::Metadata, ArrowError> {
        Metadata::deserialize(metadata)
    }

    fn supports_data_type(&self, data_type: &DataType) -> Result<(), ArrowError> {
        let (coord_type, dim) = parse_geometry_collection(data_type)?;
        if coord_type != self.coord_type {
            return Err(ArrowError::SchemaError(format!(
                "Expected coordinate type {:?}, but got {:?}",
                self.coord_type, coord_type
            )));
        }
        if dim != self.dim {
            return Err(ArrowError::SchemaError(format!(
                "Expected dimension {:?}, but got {:?}",
                self.dim, dim
            )));
        }
        Ok(())
    }

    fn try_new(data_type: &DataType, metadata: Self::Metadata) -> Result<Self, ArrowError> {
        let (coord_type, dim) = parse_geometry_collection(data_type)?;
        Ok(Self {
            coord_type,
            dim,
            metadata,
        })
    }
}

fn parse_mixed(data_type: &DataType) -> Result<(CoordType, Dimension), ArrowError> {
    match data_type {
        DataType::Union(fields, _) => {
            let mut coord_types: HashSet<CoordType> = HashSet::new();
            let mut dimensions: HashSet<Dimension> = HashSet::new();
            fields.iter().try_for_each(|(type_id, field)| {
                match type_id {
                    1 => {
                        let (ct, dim) = parse_point_data_type(field.data_type())?;
                        coord_types.insert(ct);
                        assert!(matches!(dim, Dimension::XY));
                        dimensions.insert(dim);
                    }
                    2 => {
                        let (ct, dim) = parse_linestring(field.data_type())?;
                        coord_types.insert(ct);
                        assert!(matches!(dim, Dimension::XY));
                        dimensions.insert(dim);
                    }
                    3 => {
                        let (ct, dim) = parse_polygon(field.data_type())?;
                        coord_types.insert(ct);
                        assert!(matches!(dim, Dimension::XY));
                        dimensions.insert(dim);
                    }
                    4 => {
                        let (ct, dim) = parse_multipoint_data_type(field.data_type())?;
                        coord_types.insert(ct);
                        assert!(matches!(dim, Dimension::XY));
                        dimensions.insert(dim);
                    }
                    5 => {
                        let (ct, dim) = parse_multilinestring(field.data_type())?;
                        coord_types.insert(ct);
                        assert!(matches!(dim, Dimension::XY));
                        dimensions.insert(dim);
                    }
                    6 => {
                        let (ct, dim) = parse_multipolygon(field.data_type())?;
                        coord_types.insert(ct);
                        assert!(matches!(dim, Dimension::XY));
                        dimensions.insert(dim);
                    }
                    11 => {
                        let (ct, dim) = parse_point_data_type(field.data_type())?;
                        coord_types.insert(ct);
                        assert!(matches!(dim, Dimension::XYZ));
                        dimensions.insert(dim);
                    }
                    12 => {
                        let (ct, dim) = parse_linestring(field.data_type())?;
                        coord_types.insert(ct);
                        assert!(matches!(dim, Dimension::XYZ));
                        dimensions.insert(dim);
                    }
                    13 => {
                        let (ct, dim) = parse_polygon(field.data_type())?;
                        coord_types.insert(ct);
                        assert!(matches!(dim, Dimension::XYZ));
                        dimensions.insert(dim);
                    }
                    14 => {
                        let (ct, dim) = parse_multipoint_data_type(field.data_type())?;
                        coord_types.insert(ct);
                        assert!(matches!(dim, Dimension::XYZ));
                        dimensions.insert(dim);
                    }
                    15 => {
                        let (ct, dim) = parse_multilinestring(field.data_type())?;
                        coord_types.insert(ct);
                        assert!(matches!(dim, Dimension::XYZ));
                        dimensions.insert(dim);
                    }
                    16 => {
                        let (ct, dim) = parse_multipolygon(field.data_type())?;
                        coord_types.insert(ct);
                        assert!(matches!(dim, Dimension::XYZ));
                        dimensions.insert(dim);
                    }
                    id => panic!("unexpected type id {}", id),
                };
                Ok::<_, ArrowError>(())
            })?;

            if coord_types.len() > 1 {
                return Err(ArrowError::SchemaError(
                    "Multi coord types in union".to_string(),
                ));
            }
            if dimensions.len() > 1 {
                return Err(ArrowError::SchemaError(
                    "Multi dimensions types in union".to_string(),
                ));
            }

            let coord_type = coord_types.drain().next().unwrap();
            let dimension = dimensions.drain().next().unwrap();
            Ok((coord_type, dimension))
        }
        _ => panic!("Unexpected data type"),
    }
}

fn parse_geometry_collection(data_type: &DataType) -> Result<(CoordType, Dimension), ArrowError> {
    // We need to parse the _inner_ type of the geometry collection as a union so that we can check
    // what coordinate type it's using.
    match data_type {
        DataType::List(inner_field) | DataType::LargeList(inner_field) => {
            parse_mixed(inner_field.data_type())
        }
        _ => panic!(),
    }
}

pub struct GeometryType {
    coord_type: CoordType,
    metadata: Metadata,
}

impl ExtensionType for GeometryType {
    const NAME: &'static str = "geoarrow.geometry";

    type Metadata = Metadata;

    fn metadata(&self) -> &Self::Metadata {
        &self.metadata
    }

    fn serialize_metadata(&self) -> Option<String> {
        self.metadata.serialize()
    }

    fn deserialize_metadata(metadata: Option<&str>) -> Result<Self::Metadata, ArrowError> {
        Metadata::deserialize(metadata)
    }

    fn supports_data_type(&self, data_type: &DataType) -> Result<(), ArrowError> {
        let coord_type = parse_geometry(data_type)?;
        if coord_type != self.coord_type {
            return Err(ArrowError::SchemaError(format!(
                "Expected coordinate type {:?}, but got {:?}",
                self.coord_type, coord_type
            )));
        }
        Ok(())
    }

    fn try_new(data_type: &DataType, metadata: Self::Metadata) -> Result<Self, ArrowError> {
        let coord_type = parse_geometry(data_type)?;
        Ok(Self {
            coord_type,
            metadata,
        })
    }
}

fn parse_geometry(data_type: &DataType) -> Result<CoordType, ArrowError> {
    if let DataType::Union(fields, _mode) = field.data_type() {
        let mut coord_types: HashSet<CoordType> = HashSet::new();

        fields.iter().try_for_each(|(type_id, field)| {
            match type_id {
                1 => match parse_point(field)? {
                    NativeType::Point(ct, Dimension::XY) => {
                        coord_types.insert(ct);
                    }
                    _ => unreachable!(),
                },
                2 => match parse_linestring(field)? {
                    NativeType::LineString(ct, Dimension::XY) => {
                        coord_types.insert(ct);
                    }
                    _ => unreachable!(),
                },
                3 => match parse_polygon(field)? {
                    NativeType::Polygon(ct, Dimension::XY) => {
                        coord_types.insert(ct);
                    }
                    _ => unreachable!(),
                },
                4 => match parse_multi_point(field)? {
                    NativeType::MultiPoint(ct, Dimension::XY) => {
                        coord_types.insert(ct);
                    }
                    _ => unreachable!(),
                },
                5 => match parse_multi_linestring(field)? {
                    NativeType::MultiLineString(ct, Dimension::XY) => {
                        coord_types.insert(ct);
                    }
                    _ => unreachable!(),
                },
                6 => match parse_multi_polygon(field)? {
                    NativeType::MultiPolygon(ct, Dimension::XY) => {
                        coord_types.insert(ct);
                    }
                    _ => unreachable!(),
                },
                7 => match parse_geometry_collection(field)? {
                    NativeType::GeometryCollection(ct, Dimension::XY) => {
                        coord_types.insert(ct);
                    }
                    _ => unreachable!(),
                },
                11 => match parse_point(field)? {
                    NativeType::Point(ct, Dimension::XYZ) => {
                        coord_types.insert(ct);
                    }
                    _ => unreachable!(),
                },
                12 => match parse_linestring(field)? {
                    NativeType::LineString(ct, Dimension::XYZ) => {
                        coord_types.insert(ct);
                    }
                    _ => unreachable!(),
                },
                13 => match parse_polygon(field)? {
                    NativeType::Polygon(ct, Dimension::XYZ) => {
                        coord_types.insert(ct);
                    }
                    _ => unreachable!(),
                },
                14 => match parse_multi_point(field)? {
                    NativeType::MultiPoint(ct, Dimension::XYZ) => {
                        coord_types.insert(ct);
                    }
                    _ => unreachable!(),
                },
                15 => match parse_multi_linestring(field)? {
                    NativeType::MultiLineString(ct, Dimension::XYZ) => {
                        coord_types.insert(ct);
                    }
                    _ => unreachable!(),
                },
                16 => match parse_multi_polygon(field)? {
                    NativeType::MultiPolygon(ct, Dimension::XYZ) => {
                        coord_types.insert(ct);
                    }
                    _ => unreachable!(),
                },
                17 => match parse_geometry_collection(field)? {
                    NativeType::GeometryCollection(ct, Dimension::XYZ) => {
                        coord_types.insert(ct);
                    }
                    _ => unreachable!(),
                },
                id => panic!("unexpected type id {}", id),
            };
            Ok::<_, GeoArrowError>(())
        })?;

        if coord_types.len() > 1 {
            return Err(GeoArrowError::General(
                "Multi coord types in union".to_string(),
            ));
        }

        let coord_type = coord_types.drain().next().unwrap();
        Ok(NativeType::Geometry(coord_type))
    } else {
        Err(GeoArrowError::General("Expected union type".to_string()))
    }
}

pub struct BoxType {
    dim: Dimension,
    metadata: Metadata,
}

impl ExtensionType for BoxType {
    const NAME: &'static str = "geoarrow.box";

    type Metadata = Metadata;

    fn metadata(&self) -> &Self::Metadata {
        &self.metadata
    }

    fn serialize_metadata(&self) -> Option<String> {
        self.metadata.serialize()
    }

    fn deserialize_metadata(metadata: Option<&str>) -> Result<Self::Metadata, ArrowError> {
        Metadata::deserialize(metadata)
    }

    fn supports_data_type(&self, data_type: &DataType) -> Result<(), ArrowError> {
        let dim = parse_rect(data_type)?;
        if dim != self.dim {
            return Err(ArrowError::SchemaError(format!(
                "Expected dimension {:?}, but got {:?}",
                self.dim, dim
            )));
        }
        Ok(())
    }

    fn try_new(data_type: &DataType, metadata: Self::Metadata) -> Result<Self, ArrowError> {
        let dim = parse_rect(data_type)?;
        Ok(Self { dim, metadata })
    }
}

fn parse_rect(data_type: &DataType) -> Result<Dimension, ArrowError> {
    // TODO: check child names for higher dimensions
    match data_type {
        DataType::Struct(struct_fields) => match struct_fields.len() {
            4 => Ok(Dimension::XY),
            6 => Ok(Dimension::XYZ),
            _ => panic!("unexpected number of struct fields"),
        },
        _ => panic!("unexpected data type parsing rect"),
    }
}

pub struct WkbType {
    metadata: Metadata,
}

impl ExtensionType for WkbType {
    const NAME: &'static str = "geoarrow.wkb";

    type Metadata = Metadata;

    fn metadata(&self) -> &Self::Metadata {
        &self.metadata
    }

    fn serialize_metadata(&self) -> Option<String> {
        self.metadata.serialize()
    }

    fn deserialize_metadata(metadata: Option<&str>) -> Result<Self::Metadata, ArrowError> {
        Metadata::deserialize(metadata)
    }

    fn supports_data_type(&self, data_type: &DataType) -> Result<(), ArrowError> {
        match data_type {
            DataType::Binary | DataType::LargeBinary => Ok(()),
            dt => Err(ArrowError::SchemaError(format!(
                "Unexpected data type {dt}"
            ))),
        }
    }

    fn try_new(data_type: &DataType, metadata: Self::Metadata) -> Result<Self, ArrowError> {
        let wkb = Self { metadata };
        wkb.supports_data_type(data_type)?;
        Ok(wkb)
    }
}

pub struct WktType {
    metadata: Metadata,
}

impl ExtensionType for WktType {
    const NAME: &'static str = "geoarrow.wkt";

    type Metadata = Metadata;

    fn metadata(&self) -> &Self::Metadata {
        &self.metadata
    }

    fn serialize_metadata(&self) -> Option<String> {
        self.metadata.serialize()
    }

    fn deserialize_metadata(metadata: Option<&str>) -> Result<Self::Metadata, ArrowError> {
        Metadata::deserialize(metadata)
    }

    fn supports_data_type(&self, data_type: &DataType) -> Result<(), ArrowError> {
        match data_type {
            DataType::Utf8 | DataType::LargeUtf8 => Ok(()),
            dt => Err(ArrowError::SchemaError(format!(
                "Unexpected data type {dt}"
            ))),
        }
    }

    fn try_new(data_type: &DataType, metadata: Self::Metadata) -> Result<Self, ArrowError> {
        let wkb = Self { metadata };
        wkb.supports_data_type(data_type)?;
        Ok(wkb)
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use crate::crs::Crs;
    use crate::edges::Edges;

    use super::*;
    use arrow_schema::DataType;
    use arrow_schema::Field;

    #[test]
    fn test_point_interleaved_xy() {
        let data_type =
            DataType::FixedSizeList(Arc::new(Field::new("xy", DataType::Float64, false)), 2);
        let metadata = Metadata::default();
        let type_ = PointType::try_new(&data_type, metadata).unwrap();

        assert_eq!(type_.coord_type, CoordType::Interleaved);
        assert_eq!(type_.dim, Dimension::XY);
        assert_eq!(type_.serialize_metadata(), None);
    }

    #[test]
    fn test_point_separated_xyz() {
        let data_type = DataType::Struct(
            vec![
                Field::new("x", DataType::Float64, false),
                Field::new("y", DataType::Float64, false),
                Field::new("z", DataType::Float64, false),
            ]
            .into(),
        );
        let metadata = Metadata::default();
        let type_ = PointType::try_new(&data_type, metadata).unwrap();

        assert_eq!(type_.coord_type, CoordType::Separated);
        assert_eq!(type_.dim, Dimension::XYZ);
        assert_eq!(type_.serialize_metadata(), None);
    }

    #[test]
    fn test_point_metadata() {
        let data_type =
            DataType::FixedSizeList(Arc::new(Field::new("xy", DataType::Float64, false)), 2);
        let crs = Crs::from_authority_code("EPSG:4326".to_string());
        let metadata = Metadata::new(crs, Some(Edges::Spherical));
        let type_ = PointType::try_new(&data_type, metadata).unwrap();

        let expected = r#"{"crs":"EPSG:4326","crs_type":"authority_code","edges":"spherical"}"#;
        assert_eq!(type_.serialize_metadata().as_deref(), Some(expected));
    }
}
