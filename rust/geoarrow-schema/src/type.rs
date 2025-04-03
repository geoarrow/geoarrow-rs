use std::collections::HashSet;

use arrow_schema::extension::ExtensionType;
use arrow_schema::{ArrowError, DataType};

use crate::metadata::Metadata;
use crate::{CoordType, Dimension};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
        let (coord_type, dim) = parse_point(data_type)?;
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
        let (coord_type, dim) = parse_point(data_type)?;
        Ok(Self {
            coord_type,
            dim,
            metadata,
        })
    }
}

fn parse_point(data_type: &DataType) -> Result<(CoordType, Dimension), ArrowError> {
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
            parse_point(inner_field.data_type())
        }
        dt => Err(ArrowError::SchemaError(format!(
            "Unexpected data type {dt}"
        ))),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
            DataType::List(inner2) => parse_point(inner2.data_type()),
            dt => Err(ArrowError::SchemaError(format!(
                "Unexpected inner polygon data type: {dt}"
            ))),
        },
        DataType::LargeList(inner1) => match inner1.data_type() {
            DataType::LargeList(inner2) => parse_point(inner2.data_type()),
            dt => Err(ArrowError::SchemaError(format!(
                "Unexpected inner polygon data type: {dt}"
            ))),
        },
        dt => Err(ArrowError::SchemaError(format!(
            "Unexpected root data type parsing polygon {dt}"
        ))),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
        let (coord_type, dim) = parse_multipoint(data_type)?;
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
        let (coord_type, dim) = parse_multipoint(data_type)?;
        Ok(Self {
            coord_type,
            dim,
            metadata,
        })
    }
}

fn parse_multipoint(data_type: &DataType) -> Result<(CoordType, Dimension), ArrowError> {
    match data_type {
        DataType::List(inner_field) => parse_point(inner_field.data_type()),
        DataType::LargeList(inner_field) => parse_point(inner_field.data_type()),
        dt => Err(ArrowError::SchemaError(format!(
            "Unexpected data type {dt}"
        ))),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
            DataType::List(inner2) => parse_point(inner2.data_type()),
            dt => Err(ArrowError::SchemaError(format!(
                "Unexpected inner multilinestring data type: {dt}"
            ))),
        },
        DataType::LargeList(inner1) => match inner1.data_type() {
            DataType::LargeList(inner2) => parse_point(inner2.data_type()),
            dt => Err(ArrowError::SchemaError(format!(
                "Unexpected inner multilinestring data type: {dt}"
            ))),
        },
        dt => Err(ArrowError::SchemaError(format!(
            "Unexpected data type parsing multilinestring: {dt}"
        ))),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
                DataType::List(inner3) => parse_point(inner3.data_type()),
                dt => Err(ArrowError::SchemaError(format!(
                    "Unexpected inner2 multipolygon data type: {dt}"
                ))),
            },
            dt => Err(ArrowError::SchemaError(format!(
                "Unexpected inner1 multipolygon data type: {dt}"
            ))),
        },
        DataType::LargeList(inner1) => match inner1.data_type() {
            DataType::LargeList(inner2) => match inner2.data_type() {
                DataType::LargeList(inner3) => parse_point(inner3.data_type()),
                dt => Err(ArrowError::SchemaError(format!(
                    "Unexpected inner2 multipolygon data type: {dt}"
                ))),
            },
            dt => Err(ArrowError::SchemaError(format!(
                "Unexpected inner1 multipolygon data type: {dt}"
            ))),
        },
        dt => Err(ArrowError::SchemaError(format!(
            "Unexpected data type {dt}"
        ))),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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

            // Validate that all fields of the union have the same coordinate type and dimension
            fields.iter().try_for_each(|(type_id, field)| {
                macro_rules! impl_type_id {
                    ($expected_dim:path, $parse_fn:ident) => {{
                        let (ct, dim) = $parse_fn(field.data_type())?;
                        coord_types.insert(ct);
                        assert!(matches!(dim, $expected_dim));
                        dimensions.insert(dim);
                    }};
                }

                match type_id {
                    1 => impl_type_id!(Dimension::XY, parse_point),
                    2 => impl_type_id!(Dimension::XY, parse_linestring),
                    3 => impl_type_id!(Dimension::XY, parse_polygon),
                    4 => impl_type_id!(Dimension::XY, parse_multipoint),
                    5 => impl_type_id!(Dimension::XY, parse_multilinestring),
                    6 => impl_type_id!(Dimension::XY, parse_multipolygon),
                    11 => impl_type_id!(Dimension::XYZ, parse_point),
                    12 => impl_type_id!(Dimension::XYZ, parse_linestring),
                    13 => impl_type_id!(Dimension::XYZ, parse_polygon),
                    14 => impl_type_id!(Dimension::XYZ, parse_multipoint),
                    15 => impl_type_id!(Dimension::XYZ, parse_multilinestring),
                    16 => impl_type_id!(Dimension::XYZ, parse_multipolygon),
                    21 => impl_type_id!(Dimension::XYM, parse_point),
                    22 => impl_type_id!(Dimension::XYM, parse_linestring),
                    23 => impl_type_id!(Dimension::XYM, parse_polygon),
                    24 => impl_type_id!(Dimension::XYM, parse_multipoint),
                    25 => impl_type_id!(Dimension::XYM, parse_multilinestring),
                    26 => impl_type_id!(Dimension::XYM, parse_multipolygon),
                    31 => impl_type_id!(Dimension::XYZM, parse_point),
                    32 => impl_type_id!(Dimension::XYZM, parse_linestring),
                    33 => impl_type_id!(Dimension::XYZM, parse_polygon),
                    34 => impl_type_id!(Dimension::XYZM, parse_multipoint),
                    35 => impl_type_id!(Dimension::XYZM, parse_multilinestring),
                    36 => impl_type_id!(Dimension::XYZM, parse_multipolygon),
                    id => {
                        return Err(ArrowError::SchemaError(format!(
                            "Unexpected type id parsing mixed: {id}"
                        )))
                    }
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
        dt => Err(ArrowError::SchemaError(format!(
            "Unexpected mixed data type: {dt}"
        ))),
    }
}

fn parse_geometry_collection(data_type: &DataType) -> Result<(CoordType, Dimension), ArrowError> {
    // We need to parse the _inner_ type of the geometry collection as a union so that we can check
    // what coordinate type it's using.
    match data_type {
        DataType::List(inner_field) | DataType::LargeList(inner_field) => {
            parse_mixed(inner_field.data_type())
        }
        dt => Err(ArrowError::SchemaError(format!(
            "Unexpected geometry collection data type: {dt}"
        ))),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
    if let DataType::Union(fields, _mode) = data_type {
        let mut coord_types: HashSet<CoordType> = HashSet::new();

        // Validate that all fields of the union have the same coordinate type
        fields.iter().try_for_each(|(type_id, field)| {
            macro_rules! impl_type_id {
                ($expected_dim:path, $parse_fn:ident) => {{
                    let (ct, dim) = $parse_fn(field.data_type())?;
                    coord_types.insert(ct);
                    assert!(matches!(dim, $expected_dim));
                }};
            }

            match type_id {
                1 => impl_type_id!(Dimension::XY, parse_point),
                2 => impl_type_id!(Dimension::XY, parse_linestring),
                3 => impl_type_id!(Dimension::XY, parse_polygon),
                4 => impl_type_id!(Dimension::XY, parse_multipoint),
                5 => impl_type_id!(Dimension::XY, parse_multilinestring),
                6 => impl_type_id!(Dimension::XY, parse_multipolygon),
                11 => impl_type_id!(Dimension::XYZ, parse_point),
                12 => impl_type_id!(Dimension::XYZ, parse_linestring),
                13 => impl_type_id!(Dimension::XYZ, parse_polygon),
                14 => impl_type_id!(Dimension::XYZ, parse_multipoint),
                15 => impl_type_id!(Dimension::XYZ, parse_multilinestring),
                16 => impl_type_id!(Dimension::XYZ, parse_multipolygon),
                21 => impl_type_id!(Dimension::XYM, parse_point),
                22 => impl_type_id!(Dimension::XYM, parse_linestring),
                23 => impl_type_id!(Dimension::XYM, parse_polygon),
                24 => impl_type_id!(Dimension::XYM, parse_multipoint),
                25 => impl_type_id!(Dimension::XYM, parse_multilinestring),
                26 => impl_type_id!(Dimension::XYM, parse_multipolygon),
                31 => impl_type_id!(Dimension::XYZM, parse_point),
                32 => impl_type_id!(Dimension::XYZM, parse_linestring),
                33 => impl_type_id!(Dimension::XYZM, parse_polygon),
                34 => impl_type_id!(Dimension::XYZM, parse_multipoint),
                35 => impl_type_id!(Dimension::XYZM, parse_multilinestring),
                36 => impl_type_id!(Dimension::XYZM, parse_multipolygon),
                id => {
                    return Err(ArrowError::SchemaError(format!(
                        "Unexpected type id parsing geometry: {id}"
                    )))
                }
            };
            Ok::<_, ArrowError>(())
        })?;

        if coord_types.len() > 1 {
            return Err(ArrowError::SchemaError(
                "Multi coord types in union".to_string(),
            ));
        }

        let coord_type = coord_types.drain().next().unwrap();
        Ok(coord_type)
    } else {
        Err(ArrowError::SchemaError("Expected union type".to_string()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
        let dim = parse_box(data_type)?;
        if dim != self.dim {
            return Err(ArrowError::SchemaError(format!(
                "Expected dimension {:?}, but got {:?}",
                self.dim, dim
            )));
        }
        Ok(())
    }

    fn try_new(data_type: &DataType, metadata: Self::Metadata) -> Result<Self, ArrowError> {
        let dim = parse_box(data_type)?;
        Ok(Self { dim, metadata })
    }
}

fn parse_box(data_type: &DataType) -> Result<Dimension, ArrowError> {
    match data_type {
        DataType::Struct(struct_fields) => match struct_fields.len() {
            4 => Ok(Dimension::XY),
            6 => {
                let names: HashSet<&str> =
                    struct_fields.iter().map(|f| f.name().as_str()).collect();
                if names.contains("mmin") && names.contains("mmax") {
                    Ok(Dimension::XYM)
                } else if names.contains("zmin") && names.contains("zmax") {
                    Ok(Dimension::XYZ)
                } else {
                    Err(ArrowError::SchemaError(format!("unexpected either mmin and mmax or zmin and zmax for struct with 6 fields. Got names: {:?}", names)))
                }
            }
            8 => Ok(Dimension::XYZM),
            num_fields => Err(ArrowError::SchemaError(format!(
                "unexpected number of struct fields: {}",
                num_fields
            ))),
        },
        dt => Err(ArrowError::SchemaError(format!(
            "unexpected data type parsing box: {:?}",
            dt
        ))),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
