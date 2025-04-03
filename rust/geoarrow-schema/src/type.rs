use std::collections::HashSet;
use std::sync::Arc;

use arrow_schema::extension::ExtensionType;
use arrow_schema::{ArrowError, DataType, Field, UnionFields, UnionMode};

use crate::metadata::Metadata;
use crate::{CoordType, Dimension};

macro_rules! define_basic_type {
    (
        $(#[$($attrss:meta)*])*
        $struct_name:ident
    ) => {
        $(#[$($attrss)*])*
        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        pub struct $struct_name {
            coord_type: CoordType,
            dim: Dimension,
            metadata: Arc<Metadata>,
        }

        impl $struct_name {
            /// Construct a new type from parts.
            pub fn new(coord_type: CoordType, dim: Dimension, metadata: Arc<Metadata>) -> Self {
                Self {
                    coord_type,
                    dim,
                    metadata,
                }
            }

            /// Change the underlying [`CoordType`]
            pub fn with_coord_type(self, coord_type: CoordType) -> Self {
                Self { coord_type, ..self }
            }

            /// Change the underlying [`Dimension`]
            pub fn with_dimension(self, dim: Dimension) -> Self {
                Self { dim, ..self }
            }

            /// Change the underlying [`Metadata`]
            pub fn with_metadata(self, metadata: Arc<Metadata>) -> Self {
                Self { metadata, ..self }
            }

            /// Retrieve the underlying [`CoordType`]
            pub fn coord_type(&self) -> CoordType {
                self.coord_type
            }

            /// Retrieve the underlying [`Dimension`]
            pub fn dimension(&self) -> Dimension {
                self.dim
            }

            /// Retrieve the underlying [`Metadata`]
            pub fn metadata(&self) -> &Arc<Metadata> {
                &self.metadata
            }

            /// Convert this type to a [`Field`], retaining extension metadata.
            pub fn to_field<N: Into<String>>(&self, name: N, nullable: bool) -> Field {
                Field::new(name, self.data_type(), nullable).with_extension_type(self.clone())
            }
        }
    };
}

define_basic_type!(
    /// A type representing a Point geometry, implementing the [`ExtensionType`] trait.
    PointType
);
define_basic_type!(
    /// A type representing a LineString geometry, implementing the [`ExtensionType`] trait.
    LineStringType
);
define_basic_type!(
    /// A type representing a Polygon geometry, implementing the [`ExtensionType`] trait.
    PolygonType
);
define_basic_type!(
    /// A type representing a MultiPoint geometry, implementing the [`ExtensionType`] trait.
    MultiPointType
);
define_basic_type!(
    /// A type representing a MultiLineString geometry, implementing the [`ExtensionType`] trait.
    MultiLineStringType
);
define_basic_type!(
    /// A type representing a MultiPolygon geometry, implementing the [`ExtensionType`] trait.
    MultiPolygonType
);
define_basic_type!(
    /// A type representing a GeometryCollection geometry, implementing the [`ExtensionType`] trait.
    GeometryCollectionType
);

impl PointType {
    /// Convert to the corresponding [`DataType`].
    ///
    /// Each type uniquely maps to a [`DataType`], so this is a 1:1 conversion.
    ///
    /// ```
    /// use arrow_schema::{DataType, Field};
    /// use geoarrow_schema::{CoordType, Dimension, PointType};
    ///
    /// let geom_type = PointType::new(CoordType::Interleaved, Dimension::XY, Default::default());
    /// let expected_type =
    ///     DataType::FixedSizeList(Field::new("xy", DataType::Float64, false).into(), 2);
    /// assert_eq!(geom_type.data_type(), expected_type);
    /// ```
    pub fn data_type(&self) -> DataType {
        coord_type_to_data_type(self.coord_type, self.dim)
    }
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
            metadata: Arc::new(metadata),
        })
    }
}

fn parse_point(data_type: &DataType) -> Result<(CoordType, Dimension), ArrowError> {
    match data_type {
        // TODO: use list_size for dimension when 2, or 4
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

impl LineStringType {
    /// Convert to the corresponding [`DataType`].
    ///
    /// Each type uniquely maps to a [`DataType`], so this is a 1:1 conversion.
    ///
    /// ```
    /// use arrow_schema::{DataType, Field};
    /// use geoarrow_schema::{CoordType, Dimension, LineStringType};
    ///
    /// let geom_type = LineStringType::new(CoordType::Separated, Dimension::XY, Default::default());
    /// let expected_coord_type = DataType::Struct(
    ///     vec![
    ///         Field::new("x", DataType::Float64, false),
    ///         Field::new("y", DataType::Float64, false),
    ///     ]
    ///     .into(),
    /// );
    /// let expected_type = DataType::List(Field::new("vertices", expected_coord_type, false).into());
    /// assert_eq!(geom_type.data_type(), expected_type);
    /// ```
    pub fn data_type(&self) -> DataType {
        let coords_type = coord_type_to_data_type(self.coord_type, self.dim);
        let vertices_field = Field::new("vertices", coords_type, false).into();
        DataType::List(vertices_field)
    }
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
            metadata: Arc::new(metadata),
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

impl PolygonType {
    /// Convert to the corresponding [`DataType`].
    ///
    /// Each type uniquely maps to a [`DataType`], so this is a 1:1 conversion.
    ///
    /// ```
    /// use arrow_schema::{DataType, Field};
    /// use geoarrow_schema::{CoordType, Dimension, PolygonType};
    ///
    /// let geom_type = PolygonType::new(CoordType::Separated, Dimension::XYZ, Default::default());
    ///
    /// let expected_coord_type = DataType::Struct(
    ///     vec![
    ///         Field::new("x", DataType::Float64, false),
    ///         Field::new("y", DataType::Float64, false),
    ///         Field::new("z", DataType::Float64, false),
    ///     ]
    ///     .into(),
    /// );
    /// let vertices_field = Field::new("vertices", expected_coord_type, false);
    /// let rings_field = Field::new_list("rings", vertices_field, false);
    /// let expected_type = DataType::List(rings_field.into());
    /// assert_eq!(geom_type.data_type(), expected_type);
    /// ```
    pub fn data_type(&self) -> DataType {
        let coords_type = coord_type_to_data_type(self.coord_type, self.dim);
        let vertices_field = Field::new("vertices", coords_type, false);
        let rings_field = Field::new_list("rings", vertices_field, false).into();
        DataType::List(rings_field)
    }
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
            metadata: Arc::new(metadata),
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

impl MultiPointType {
    /// Convert to the corresponding [`DataType`].
    ///
    /// Each type uniquely maps to a [`DataType`], so this is a 1:1 conversion.
    ///
    /// ```
    /// use arrow_schema::{DataType, Field};
    /// use geoarrow_schema::{CoordType, Dimension, MultiPointType};
    ///
    /// let geom_type = MultiPointType::new(CoordType::Separated, Dimension::XYZ, Default::default());
    ///
    /// let expected_coord_type = DataType::Struct(
    ///     vec![
    ///         Field::new("x", DataType::Float64, false),
    ///         Field::new("y", DataType::Float64, false),
    ///         Field::new("z", DataType::Float64, false),
    ///     ]
    ///     .into(),
    /// );
    /// let vertices_field = Field::new("points", expected_coord_type, false);
    /// let expected_type = DataType::List(vertices_field.into());
    /// assert_eq!(geom_type.data_type(), expected_type);
    /// ```
    pub fn data_type(&self) -> DataType {
        let coords_type = coord_type_to_data_type(self.coord_type, self.dim);
        let vertices_field = Field::new("points", coords_type, false).into();
        DataType::List(vertices_field)
    }
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
            metadata: Arc::new(metadata),
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

impl MultiLineStringType {
    /// Convert to the corresponding [`DataType`].
    ///
    /// Each type uniquely maps to a [`DataType`], so this is a 1:1 conversion.
    ///
    /// ```
    /// use arrow_schema::{DataType, Field};
    /// use geoarrow_schema::{CoordType, Dimension, MultiLineStringType};
    ///
    /// let geom_type =
    ///     MultiLineStringType::new(CoordType::Separated, Dimension::XYZ, Default::default());
    ///
    /// let expected_coord_type = DataType::Struct(
    ///     vec![
    ///         Field::new("x", DataType::Float64, false),
    ///         Field::new("y", DataType::Float64, false),
    ///         Field::new("z", DataType::Float64, false),
    ///     ]
    ///     .into(),
    /// );
    /// let vertices_field = Field::new("vertices", expected_coord_type, false);
    /// let linestrings_field = Field::new_list("linestrings", vertices_field, false);
    /// let expected_type = DataType::List(linestrings_field.into());
    /// assert_eq!(geom_type.data_type(), expected_type);
    /// ```
    pub fn data_type(&self) -> DataType {
        let coords_type = coord_type_to_data_type(self.coord_type, self.dim);
        let vertices_field = Field::new("vertices", coords_type, false);
        let linestrings_field = Field::new_list("linestrings", vertices_field, false).into();
        DataType::List(linestrings_field)
    }
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
            metadata: Arc::new(metadata),
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

impl MultiPolygonType {
    /// Convert to the corresponding [`DataType`].
    ///
    /// Each type uniquely maps to a [`DataType`], so this is a 1:1 conversion.
    ///
    /// ```
    /// use arrow_schema::{DataType, Field};
    /// use geoarrow_schema::{CoordType, Dimension, MultiPolygonType};
    ///
    /// let geom_type = MultiPolygonType::new(CoordType::Separated, Dimension::XYM, Default::default());
    ///
    /// let expected_coord_type = DataType::Struct(
    ///     vec![
    ///         Field::new("x", DataType::Float64, false),
    ///         Field::new("y", DataType::Float64, false),
    ///         Field::new("m", DataType::Float64, false),
    ///     ]
    ///     .into(),
    /// );
    /// let vertices_field = Field::new("vertices", expected_coord_type, false);
    /// let rings_field = Field::new_list("rings", vertices_field, false);
    /// let polygons_field = Field::new_list("polygons", rings_field, false);
    /// let expected_type = DataType::List(polygons_field.into());
    /// assert_eq!(geom_type.data_type(), expected_type);
    /// ```
    pub fn data_type(&self) -> DataType {
        let coords_type = coord_type_to_data_type(self.coord_type, self.dim);
        let vertices_field = Field::new("vertices", coords_type, false);
        let rings_field = Field::new_list("rings", vertices_field, false);
        let polygons_field = Field::new_list("polygons", rings_field, false).into();
        DataType::List(polygons_field)
    }
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
            metadata: Arc::new(metadata),
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

impl GeometryCollectionType {
    /// Convert to the corresponding [`DataType`].
    ///
    /// Each type uniquely maps to a [`DataType`], so this is a 1:1 conversion.
    ///
    /// ```
    /// use std::sync::Arc;
    ///
    /// use arrow_schema::{DataType, Field, UnionFields, UnionMode};
    /// use geoarrow_schema::{
    ///     CoordType, Dimension, GeometryCollectionType, LineStringType, Metadata, MultiLineStringType,
    ///     MultiPointType, MultiPolygonType, PointType, PolygonType,
    /// };
    ///
    /// let coord_type = CoordType::Interleaved;
    /// let dim = Dimension::XY;
    /// let metadata = Arc::new(Metadata::default());
    /// let geom_type = GeometryCollectionType::new(coord_type, dim, metadata.clone());
    ///
    /// let fields = vec![
    ///     Field::new(
    ///         "Point",
    ///         PointType::new(coord_type, dim, metadata.clone()).data_type(),
    ///         true,
    ///     ),
    ///     Field::new(
    ///         "LineString",
    ///         LineStringType::new(coord_type, dim, metadata.clone()).data_type(),
    ///         true,
    ///     ),
    ///     Field::new(
    ///         "Polygon",
    ///         PolygonType::new(coord_type, dim, metadata.clone()).data_type(),
    ///         true,
    ///     ),
    ///     Field::new(
    ///         "MultiPoint",
    ///         MultiPointType::new(coord_type, dim, metadata.clone()).data_type(),
    ///         true,
    ///     ),
    ///     Field::new(
    ///         "MultiLineString",
    ///         MultiLineStringType::new(coord_type, dim, metadata.clone()).data_type(),
    ///         true,
    ///     ),
    ///     Field::new(
    ///         "MultiPolygon",
    ///         MultiPolygonType::new(coord_type, dim, metadata.clone()).data_type(),
    ///         true,
    ///     ),
    /// ];
    /// let type_ids = vec![1, 2, 3, 4, 5, 6];
    ///
    /// let union_fields = UnionFields::new(type_ids, fields);
    /// let union_data_type = DataType::Union(union_fields, UnionMode::Dense);
    ///
    /// let geometries_field = Field::new("geometries", union_data_type, false).into();
    /// let expected_type = DataType::List(geometries_field);
    ///
    /// assert_eq!(geom_type.data_type(), expected_type);
    /// ```
    pub fn data_type(&self) -> DataType {
        let geometries_field = Field::new(
            "geometries",
            mixed_data_type(self.coord_type, self.dim),
            false,
        )
        .into();
        DataType::List(geometries_field)
    }
}

fn mixed_data_type(coord_type: CoordType, dim: Dimension) -> DataType {
    let mut fields = vec![];
    let mut type_ids = vec![];

    match dim {
        Dimension::XY => type_ids.extend([1, 2, 3, 4, 5, 6]),
        Dimension::XYZ => type_ids.extend([11, 12, 13, 14, 15, 16]),
        Dimension::XYM => type_ids.extend([21, 22, 23, 24, 25, 26]),
        Dimension::XYZM => type_ids.extend([31, 32, 33, 34, 35, 36]),
    }

    // Note: we manually construct the fields because these fields shouldn't have their own
    // GeoArrow extension metadata
    macro_rules! push_field {
        ($field_name:literal, $geom_type:ident) => {{
            fields.push(Field::new(
                $field_name,
                $geom_type {
                    coord_type,
                    dim,
                    metadata: Metadata::default().into(),
                }
                .data_type(),
                true,
            ));
        }};
    }

    match dim {
        Dimension::XY => {
            push_field!("Point", PointType);
            push_field!("LineString", LineStringType);
            push_field!("Polygon", PolygonType);
            push_field!("MultiPoint", MultiPointType);
            push_field!("MultiLineString", MultiLineStringType);
            push_field!("MultiPolygon", MultiPolygonType);
        }
        Dimension::XYZ => {
            push_field!("Point Z", PointType);
            push_field!("LineString Z", LineStringType);
            push_field!("Polygon Z", PolygonType);
            push_field!("MultiPoint Z", MultiPointType);
            push_field!("MultiLineString Z", MultiLineStringType);
            push_field!("MultiPolygon Z", MultiPolygonType);
        }
        Dimension::XYM => {
            push_field!("Point M", PointType);
            push_field!("LineString M", LineStringType);
            push_field!("Polygon M", PolygonType);
            push_field!("MultiPoint M", MultiPointType);
            push_field!("MultiLineString M", MultiLineStringType);
            push_field!("MultiPolygon M", MultiPolygonType);
        }
        Dimension::XYZM => {
            push_field!("Point ZM", PointType);
            push_field!("LineString ZM", LineStringType);
            push_field!("Polygon ZM", PolygonType);
            push_field!("MultiPoint ZM", MultiPointType);
            push_field!("MultiLineString ZM", MultiLineStringType);
            push_field!("MultiPolygon ZM", MultiPolygonType);
        }
    }

    let union_fields = UnionFields::new(type_ids, fields);
    DataType::Union(union_fields, UnionMode::Dense)
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
            metadata: Arc::new(metadata),
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
    metadata: Arc<Metadata>,
}

impl GeometryType {
    /// Construct a new type from parts.
    pub fn new(coord_type: CoordType, metadata: Arc<Metadata>) -> Self {
        Self {
            coord_type,
            metadata,
        }
    }

    /// Change the underlying [`CoordType`]
    pub fn with_coord_type(self, coord_type: CoordType) -> Self {
        Self { coord_type, ..self }
    }

    /// Change the underlying [`Metadata`]
    pub fn with_metadata(self, metadata: Arc<Metadata>) -> Self {
        Self { metadata, ..self }
    }

    /// Retrieve the underlying [`CoordType`]
    pub fn coord_type(&self) -> CoordType {
        self.coord_type
    }

    /// Retrieve the underlying [`Metadata`]
    pub fn metadata(&self) -> &Arc<Metadata> {
        &self.metadata
    }

    /// Convert to the corresponding [`DataType`].
    ///
    /// Each type uniquely maps to a [`DataType`], so this is a 1:1 conversion.
    pub fn data_type(&self) -> DataType {
        let mut fields = vec![];
        let type_ids = vec![
            1, 2, 3, 4, 5, 6, 7, 11, 12, 13, 14, 15, 16, 17, 21, 22, 23, 24, 25, 26, 27, 31, 32,
            33, 34, 35, 36, 37,
        ];

        // Note: we manually construct the fields because these fields shouldn't have their own
        // GeoArrow extension metadata
        macro_rules! push_field {
            ($field_name:literal, $geom_type:ident, $dim:path) => {{
                fields.push(Field::new(
                    $field_name,
                    $geom_type {
                        coord_type: self.coord_type,
                        dim: $dim,
                        metadata: Metadata::default().into(),
                    }
                    .data_type(),
                    true,
                ));
            }};
        }

        push_field!("Point", PointType, Dimension::XY);
        push_field!("LineString", LineStringType, Dimension::XY);
        push_field!("Polygon", PolygonType, Dimension::XY);
        push_field!("MultiPoint", MultiPointType, Dimension::XY);
        push_field!("MultiLineString", MultiLineStringType, Dimension::XY);
        push_field!("MultiPolygon", MultiPolygonType, Dimension::XY);
        push_field!("GeometryCollection", GeometryCollectionType, Dimension::XY);

        push_field!("Point Z", PointType, Dimension::XYZ);
        push_field!("LineString Z", LineStringType, Dimension::XYZ);
        push_field!("Polygon Z", PolygonType, Dimension::XYZ);
        push_field!("MultiPoint Z", MultiPointType, Dimension::XYZ);
        push_field!("MultiLineString Z", MultiLineStringType, Dimension::XYZ);
        push_field!("MultiPolygon Z", MultiPolygonType, Dimension::XYZ);
        push_field!(
            "GeometryCollection Z",
            GeometryCollectionType,
            Dimension::XYZ
        );

        push_field!("Point M", PointType, Dimension::XYM);
        push_field!("LineString M", LineStringType, Dimension::XYM);
        push_field!("Polygon M", PolygonType, Dimension::XYM);
        push_field!("MultiPoint M", MultiPointType, Dimension::XYM);
        push_field!("MultiLineString M", MultiLineStringType, Dimension::XYM);
        push_field!("MultiPolygon M", MultiPolygonType, Dimension::XYM);
        push_field!(
            "GeometryCollection M",
            GeometryCollectionType,
            Dimension::XYM
        );

        push_field!("Point ZM", PointType, Dimension::XYZM);
        push_field!("LineString ZM", LineStringType, Dimension::XYZM);
        push_field!("Polygon ZM", PolygonType, Dimension::XYZM);
        push_field!("MultiPoint ZM", MultiPointType, Dimension::XYZM);
        push_field!("MultiLineString ZM", MultiLineStringType, Dimension::XYZM);
        push_field!("MultiPolygon ZM", MultiPolygonType, Dimension::XYZM);
        push_field!(
            "GeometryCollection ZM",
            GeometryCollectionType,
            Dimension::XYZM
        );

        let union_fields = UnionFields::new(type_ids, fields);
        DataType::Union(union_fields, UnionMode::Dense)
    }

    /// Convert this type to a [`Field`], retaining extension metadata.
    pub fn to_field<N: Into<String>>(&self, name: N, nullable: bool) -> Field {
        Field::new(name, self.data_type(), nullable).with_extension_type(self.clone())
    }
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
            metadata: Arc::new(metadata),
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
                7 => impl_type_id!(Dimension::XY, parse_geometry_collection),
                11 => impl_type_id!(Dimension::XYZ, parse_point),
                12 => impl_type_id!(Dimension::XYZ, parse_linestring),
                13 => impl_type_id!(Dimension::XYZ, parse_polygon),
                14 => impl_type_id!(Dimension::XYZ, parse_multipoint),
                15 => impl_type_id!(Dimension::XYZ, parse_multilinestring),
                16 => impl_type_id!(Dimension::XYZ, parse_multipolygon),
                17 => impl_type_id!(Dimension::XYZ, parse_geometry_collection),
                21 => impl_type_id!(Dimension::XYM, parse_point),
                22 => impl_type_id!(Dimension::XYM, parse_linestring),
                23 => impl_type_id!(Dimension::XYM, parse_polygon),
                24 => impl_type_id!(Dimension::XYM, parse_multipoint),
                25 => impl_type_id!(Dimension::XYM, parse_multilinestring),
                26 => impl_type_id!(Dimension::XYM, parse_multipolygon),
                27 => impl_type_id!(Dimension::XYM, parse_geometry_collection),
                31 => impl_type_id!(Dimension::XYZM, parse_point),
                32 => impl_type_id!(Dimension::XYZM, parse_linestring),
                33 => impl_type_id!(Dimension::XYZM, parse_polygon),
                34 => impl_type_id!(Dimension::XYZM, parse_multipoint),
                35 => impl_type_id!(Dimension::XYZM, parse_multilinestring),
                36 => impl_type_id!(Dimension::XYZM, parse_multipolygon),
                37 => impl_type_id!(Dimension::XYZM, parse_geometry_collection),
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
    metadata: Arc<Metadata>,
}

impl BoxType {
    /// Construct a new type from parts.
    pub fn new(dim: Dimension, metadata: Arc<Metadata>) -> Self {
        Self { dim, metadata }
    }

    /// Change the underlying [`Dimension`]
    pub fn with_dimension(self, dim: Dimension) -> Self {
        Self { dim, ..self }
    }

    /// Change the underlying [`Metadata`]
    pub fn with_metadata(self, metadata: Arc<Metadata>) -> Self {
        Self { metadata, ..self }
    }

    /// Retrieve the underlying [`CoordType`]
    pub fn dimension(&self) -> Dimension {
        self.dim
    }

    /// Retrieve the underlying [`Metadata`]
    pub fn metadata(&self) -> &Arc<Metadata> {
        &self.metadata
    }

    /// Convert to the corresponding [`DataType`].
    ///
    /// Each type uniquely maps to a [`DataType`], so this is a 1:1 conversion.
    ///
    /// ```
    /// use arrow_schema::{DataType, Field};
    /// use geoarrow_schema::{BoxType, Dimension};
    ///
    /// let geom_type = BoxType::new(Dimension::XYZM, Default::default());
    ///
    /// let expected_type = DataType::Struct(
    ///     vec![
    ///         Field::new("xmin", DataType::Float64, false),
    ///         Field::new("ymin", DataType::Float64, false),
    ///         Field::new("zmin", DataType::Float64, false),
    ///         Field::new("mmin", DataType::Float64, false),
    ///         Field::new("xmax", DataType::Float64, false),
    ///         Field::new("ymax", DataType::Float64, false),
    ///         Field::new("zmax", DataType::Float64, false),
    ///         Field::new("mmax", DataType::Float64, false),
    ///     ]
    ///     .into(),
    /// );
    /// assert_eq!(geom_type.data_type(), expected_type);
    /// ```
    pub fn data_type(&self) -> DataType {
        let values_fields = match self.dim {
            Dimension::XY => {
                vec![
                    Field::new("xmin", DataType::Float64, false),
                    Field::new("ymin", DataType::Float64, false),
                    Field::new("xmax", DataType::Float64, false),
                    Field::new("ymax", DataType::Float64, false),
                ]
            }
            Dimension::XYZ => {
                vec![
                    Field::new("xmin", DataType::Float64, false),
                    Field::new("ymin", DataType::Float64, false),
                    Field::new("zmin", DataType::Float64, false),
                    Field::new("xmax", DataType::Float64, false),
                    Field::new("ymax", DataType::Float64, false),
                    Field::new("zmax", DataType::Float64, false),
                ]
            }
            Dimension::XYM => {
                vec![
                    Field::new("xmin", DataType::Float64, false),
                    Field::new("ymin", DataType::Float64, false),
                    Field::new("mmin", DataType::Float64, false),
                    Field::new("xmax", DataType::Float64, false),
                    Field::new("ymax", DataType::Float64, false),
                    Field::new("mmax", DataType::Float64, false),
                ]
            }
            Dimension::XYZM => {
                vec![
                    Field::new("xmin", DataType::Float64, false),
                    Field::new("ymin", DataType::Float64, false),
                    Field::new("zmin", DataType::Float64, false),
                    Field::new("mmin", DataType::Float64, false),
                    Field::new("xmax", DataType::Float64, false),
                    Field::new("ymax", DataType::Float64, false),
                    Field::new("zmax", DataType::Float64, false),
                    Field::new("mmax", DataType::Float64, false),
                ]
            }
        };
        DataType::Struct(values_fields.into())
    }

    /// Convert this type to a [`Field`], retaining extension metadata.
    pub fn to_field<N: Into<String>>(&self, name: N, nullable: bool) -> Field {
        Field::new(name, self.data_type(), nullable).with_extension_type(self.clone())
    }
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
        Ok(Self {
            dim,
            metadata: Arc::new(metadata),
        })
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
    metadata: Arc<Metadata>,
}

impl WkbType {
    /// Construct a new type from parts.
    pub fn new(metadata: Arc<Metadata>) -> Self {
        Self { metadata }
    }

    /// Change the underlying [`Metadata`]
    pub fn with_metadata(self, metadata: Arc<Metadata>) -> Self {
        Self { metadata }
    }

    /// Retrieve the underlying [`Metadata`]
    pub fn metadata(&self) -> &Arc<Metadata> {
        &self.metadata
    }

    /// Convert to the corresponding [`DataType`].
    ///
    /// Each type uniquely maps to a [`DataType`], so this is a 1:1 conversion.
    ///
    /// ```
    /// use arrow_schema::DataType;
    /// use geoarrow_schema::WkbType;
    ///
    /// let geom_type = WkbType::new(Default::default());
    ///
    /// assert_eq!(geom_type.data_type(false), DataType::Binary);
    /// ```
    pub fn data_type(&self, large: bool) -> DataType {
        if large {
            DataType::LargeBinary
        } else {
            DataType::Binary
        }
    }

    /// Convert this type to a [`Field`], retaining extension metadata.
    pub fn to_field<N: Into<String>>(&self, name: N, nullable: bool, large: bool) -> Field {
        Field::new(name, self.data_type(large), nullable).with_extension_type(self.clone())
    }
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
        let wkb = Self {
            metadata: Arc::new(metadata),
        };
        wkb.supports_data_type(data_type)?;
        Ok(wkb)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WktType {
    metadata: Arc<Metadata>,
}

impl WktType {
    /// Construct a new type from parts.
    pub fn new(metadata: Arc<Metadata>) -> Self {
        Self { metadata }
    }

    /// Change the underlying [`Metadata`]
    pub fn with_metadata(self, metadata: Arc<Metadata>) -> Self {
        Self { metadata }
    }

    /// Retrieve the underlying [`Metadata`]
    pub fn metadata(&self) -> &Arc<Metadata> {
        &self.metadata
    }

    /// Convert to the corresponding [`DataType`].
    ///
    /// Each type uniquely maps to a [`DataType`], so this is a 1:1 conversion.
    ///
    /// ```
    /// use arrow_schema::DataType;
    /// use geoarrow_schema::WktType;
    ///
    /// let geom_type = WktType::new(Default::default());
    ///
    /// assert_eq!(geom_type.data_type(false), DataType::Utf8);
    /// ```
    pub fn data_type(&self, large: bool) -> DataType {
        if large {
            DataType::LargeUtf8
        } else {
            DataType::Utf8
        }
    }

    /// Convert this type to a [`Field`], retaining extension metadata.
    pub fn to_field<N: Into<String>>(&self, name: N, nullable: bool, large: bool) -> Field {
        Field::new(name, self.data_type(large), nullable).with_extension_type(self.clone())
    }
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
        let wkb = Self {
            metadata: Arc::new(metadata),
        };
        wkb.supports_data_type(data_type)?;
        Ok(wkb)
    }
}

fn coord_type_to_data_type(coord_type: CoordType, dim: Dimension) -> DataType {
    match (coord_type, dim) {
        (CoordType::Interleaved, Dimension::XY) => {
            let values_field = Field::new("xy", DataType::Float64, false);
            DataType::FixedSizeList(Arc::new(values_field), 2)
        }
        (CoordType::Interleaved, Dimension::XYZ) => {
            let values_field = Field::new("xyz", DataType::Float64, false);
            DataType::FixedSizeList(Arc::new(values_field), 3)
        }
        (CoordType::Interleaved, Dimension::XYM) => {
            let values_field = Field::new("xym", DataType::Float64, false);
            DataType::FixedSizeList(Arc::new(values_field), 3)
        }
        (CoordType::Interleaved, Dimension::XYZM) => {
            let values_field = Field::new("xyzm", DataType::Float64, false);
            DataType::FixedSizeList(Arc::new(values_field), 4)
        }
        (CoordType::Separated, Dimension::XY) => {
            let values_fields = vec![
                Field::new("x", DataType::Float64, false),
                Field::new("y", DataType::Float64, false),
            ];
            DataType::Struct(values_fields.into())
        }
        (CoordType::Separated, Dimension::XYZ) => {
            let values_fields = vec![
                Field::new("x", DataType::Float64, false),
                Field::new("y", DataType::Float64, false),
                Field::new("z", DataType::Float64, false),
            ];
            DataType::Struct(values_fields.into())
        }
        (CoordType::Separated, Dimension::XYM) => {
            let values_fields = vec![
                Field::new("x", DataType::Float64, false),
                Field::new("y", DataType::Float64, false),
                Field::new("m", DataType::Float64, false),
            ];
            DataType::Struct(values_fields.into())
        }
        (CoordType::Separated, Dimension::XYZM) => {
            let values_fields = vec![
                Field::new("x", DataType::Float64, false),
                Field::new("y", DataType::Float64, false),
                Field::new("z", DataType::Float64, false),
                Field::new("m", DataType::Float64, false),
            ];
            DataType::Struct(values_fields.into())
        }
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

    #[test]
    fn geometry_data_type() {
        let typ =
            GeometryCollectionType::new(CoordType::Interleaved, Dimension::XY, Default::default());
        dbg!(typ.data_type());
    }
}
