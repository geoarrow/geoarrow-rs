use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, OffsetSizeTrait, UnionArray};
use arrow_buffer::{NullBuffer, ScalarBuffer};
use arrow_schema::extension::{EXTENSION_TYPE_METADATA_KEY, EXTENSION_TYPE_NAME_KEY};
use arrow_schema::{DataType, Field, UnionMode};
use geo_traits::GeometryTrait;
use geoarrow_schema::{CoordType, Dimension, Metadata};

use crate::algorithm::native::downcast::can_downcast_multi;
use crate::array::mixed::builder::MixedGeometryBuilder;
use crate::array::mixed::MixedCapacity;
use crate::array::{
    GeometryCollectionArray, LineStringArray, LineStringBuilder, MultiLineStringArray,
    MultiLineStringBuilder, MultiPointArray, MultiPointBuilder, MultiPolygonArray,
    MultiPolygonBuilder, PointArray, PointBuilder, PolygonArray, PolygonBuilder, WKBArray,
};
use crate::datatypes::NativeType;
use crate::error::{GeoArrowError, Result};
use crate::scalar::Geometry;
use crate::trait_::{ArrayAccessor, GeometryArraySelfMethods, IntoArrow, NativeGeometryAccessor};
use crate::{ArrayBase, NativeArray};

/// # Invariants
///
/// - All arrays must have the same dimension
/// - All arrays must have the same coordinate layout (interleaved or separated)
///
/// - 1: Point
/// - 2: LineString
/// - 3: Polygon
/// - 4: MultiPoint
/// - 5: MultiLineString
/// - 6: MultiPolygon
/// - 7: GeometryCollection
/// - 11: Point Z
/// - 12: LineString Z
/// - 13: Polygon Z
/// - 14: MultiPoint Z
/// - 15: MultiLineString Z
/// - 16: MultiPolygon Z
/// - 17: GeometryCollection Z
/// - 21: Point M
/// - 22: LineString M
/// - 23: Polygon M
/// - 24: MultiPoint M
/// - 25: MultiLineString M
/// - 26: MultiPolygon M
/// - 27: GeometryCollection M
/// - 31: Point ZM
/// - 32: LineString ZM
/// - 33: Polygon ZM
/// - 34: MultiPoint ZM
/// - 35: MultiLineString ZM
/// - 36: MultiPolygon ZM
/// - 37: GeometryCollection ZM
#[derive(Debug, Clone, PartialEq)]
pub struct MixedGeometryArray {
    // We store the coord type and dimension separately because there's no NativeType::Mixed
    // variant
    coord_type: CoordType,
    dim: Dimension,

    pub(crate) metadata: Arc<Metadata>,

    /// Invariant: every item in `type_ids` is `> 0 && < fields.len()` if `type_ids` are not provided. If `type_ids` exist in the NativeType, then every item in `type_ids` is `> 0 && `
    pub(crate) type_ids: ScalarBuffer<i8>,

    /// Invariant: `offsets.len() == type_ids.len()`
    pub(crate) offsets: ScalarBuffer<i32>,

    /// Invariant: Any of these arrays that are `Some()` must have length >0
    pub(crate) points: PointArray,
    pub(crate) line_strings: LineStringArray,
    pub(crate) polygons: PolygonArray,
    pub(crate) multi_points: MultiPointArray,
    pub(crate) multi_line_strings: MultiLineStringArray,
    pub(crate) multi_polygons: MultiPolygonArray,

    /// We don't need a separate slice_length, because that's the length of the full
    /// MixedGeometryArray
    slice_offset: usize,
}

impl MixedGeometryArray {
    /// Create a new MixedGeometryArray from parts
    ///
    /// # Implementation
    ///
    /// This function is `O(1)`.
    ///
    /// # Panics
    ///
    /// - if the validity is not `None` and its length is different from the number of geometries
    /// - if the largest geometry offset does not match the number of coordinates
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        type_ids: ScalarBuffer<i8>,
        offsets: ScalarBuffer<i32>,
        points: Option<PointArray>,
        line_strings: Option<LineStringArray>,
        polygons: Option<PolygonArray>,
        multi_points: Option<MultiPointArray>,
        multi_line_strings: Option<MultiLineStringArray>,
        multi_polygons: Option<MultiPolygonArray>,
        metadata: Arc<Metadata>,
    ) -> Self {
        let mut coord_types = HashSet::new();
        if let Some(points) = &points {
            coord_types.insert(points.coord_type());
        }
        if let Some(line_strings) = &line_strings {
            coord_types.insert(line_strings.coord_type());
        }
        if let Some(polygons) = &polygons {
            coord_types.insert(polygons.coord_type());
        }
        if let Some(multi_points) = &multi_points {
            coord_types.insert(multi_points.coord_type());
        }
        if let Some(multi_line_strings) = &multi_line_strings {
            coord_types.insert(multi_line_strings.coord_type());
        }
        if let Some(multi_polygons) = &multi_polygons {
            coord_types.insert(multi_polygons.coord_type());
        }
        assert!(coord_types.len() <= 1);
        let coord_type = coord_types
            .into_iter()
            .next()
            .unwrap_or(CoordType::Interleaved);

        let mut dimensions = HashSet::new();
        if let Some(points) = &points {
            dimensions.insert(points.dimension());
        }
        if let Some(line_strings) = &line_strings {
            dimensions.insert(line_strings.dimension());
        }
        if let Some(polygons) = &polygons {
            dimensions.insert(polygons.dimension());
        }
        if let Some(multi_points) = &multi_points {
            dimensions.insert(multi_points.dimension());
        }
        if let Some(multi_line_strings) = &multi_line_strings {
            dimensions.insert(multi_line_strings.dimension());
        }
        if let Some(multi_polygons) = &multi_polygons {
            dimensions.insert(multi_polygons.dimension());
        }
        assert_eq!(dimensions.len(), 1);
        let dim = dimensions.into_iter().next().unwrap();

        Self {
            coord_type,
            dim,
            type_ids,
            offsets,
            points: points.unwrap_or(
                PointBuilder::new_with_options(dim, coord_type, Default::default()).finish(),
            ),
            line_strings: line_strings.unwrap_or(
                LineStringBuilder::new_with_options(dim, coord_type, Default::default()).finish(),
            ),
            polygons: polygons.unwrap_or(
                PolygonBuilder::new_with_options(dim, coord_type, Default::default()).finish(),
            ),
            multi_points: multi_points.unwrap_or(
                MultiPointBuilder::new_with_options(dim, coord_type, Default::default()).finish(),
            ),
            multi_line_strings: multi_line_strings.unwrap_or(
                MultiLineStringBuilder::new_with_options(dim, coord_type, Default::default())
                    .finish(),
            ),
            multi_polygons: multi_polygons.unwrap_or(
                MultiPolygonBuilder::new_with_options(dim, coord_type, Default::default()).finish(),
            ),
            metadata,
            slice_offset: 0,
        }
    }

    /// The lengths of each buffer contained in this array.
    pub fn buffer_lengths(&self) -> MixedCapacity {
        MixedCapacity::new(
            self.points.buffer_lengths(),
            self.line_strings.buffer_lengths(),
            self.polygons.buffer_lengths(),
            self.multi_points.buffer_lengths(),
            self.multi_line_strings.buffer_lengths(),
            self.multi_polygons.buffer_lengths(),
        )
    }

    /// Return `true` if this array has been sliced.
    pub(crate) fn is_sliced(&self) -> bool {
        // Note this is still not a valid check, because it could've been sliced with start 0 but
        // length less than the full length.
        // self.slice_offset > 0 || self.slice_length

        let mut child_lengths = 0;
        child_lengths += self.points.len();
        child_lengths += self.line_strings.len();
        child_lengths += self.polygons.len();
        child_lengths += self.multi_points.len();
        child_lengths += self.multi_line_strings.len();
        child_lengths += self.multi_polygons.len();

        child_lengths > self.len()
    }

    /// The offset and length by which this array has been sliced.
    ///
    /// If this array has not been sliced, the slice offset will be `0`. The length will always be
    /// equal to `self.len()`.
    pub(crate) fn slice_offset_length(&self) -> (usize, usize) {
        (self.slice_offset, self.len())
    }

    pub fn has_points(&self) -> bool {
        if self.points.is_empty() {
            return false;
        }

        // If the array has been sliced, check a point type id still exists
        if self.is_sliced() {
            for t in self.type_ids.iter() {
                if *t % 10 == 1 {
                    return true;
                }
            }

            return false;
        }

        true
    }

    pub fn has_line_strings(&self) -> bool {
        if self.line_strings.is_empty() {
            return false;
        }

        // If the array has been sliced, check a point type id still exists
        if self.is_sliced() {
            for t in self.type_ids.iter() {
                if *t % 10 == 2 {
                    return true;
                }
            }

            return false;
        }

        true
    }

    pub fn has_polygons(&self) -> bool {
        if self.polygons.is_empty() {
            return false;
        }

        // If the array has been sliced, check a point type id still exists
        if self.is_sliced() {
            for t in self.type_ids.iter() {
                if *t % 10 == 3 {
                    return true;
                }
            }

            return false;
        }

        true
    }

    pub fn has_multi_points(&self) -> bool {
        if self.multi_points.is_empty() {
            return false;
        }

        // If the array has been sliced, check a point type id still exists
        if self.is_sliced() {
            for t in self.type_ids.iter() {
                if *t % 10 == 4 {
                    return true;
                }
            }

            return false;
        }

        true
    }

    pub fn has_multi_line_strings(&self) -> bool {
        if self.multi_line_strings.is_empty() {
            return false;
        }

        // If the array has been sliced, check a point type id still exists
        if self.is_sliced() {
            for t in self.type_ids.iter() {
                if *t % 10 == 5 {
                    return true;
                }
            }

            return false;
        }

        true
    }

    pub fn has_multi_polygons(&self) -> bool {
        if self.multi_polygons.is_empty() {
            return false;
        }

        // If the array has been sliced, check a point type id still exists
        if self.is_sliced() {
            for t in self.type_ids.iter() {
                if *t % 10 == 6 {
                    return true;
                }
            }

            return false;
        }

        true
    }

    pub fn has_only_points(&self) -> bool {
        self.has_points()
            && !self.has_line_strings()
            && !self.has_polygons()
            && !self.has_multi_points()
            && !self.has_multi_line_strings()
            && !self.has_multi_polygons()
    }

    pub fn has_only_line_strings(&self) -> bool {
        !self.has_points()
            && self.has_line_strings()
            && !self.has_polygons()
            && !self.has_multi_points()
            && !self.has_multi_line_strings()
            && !self.has_multi_polygons()
    }

    pub fn has_only_polygons(&self) -> bool {
        !self.has_points()
            && !self.has_line_strings()
            && self.has_polygons()
            && !self.has_multi_points()
            && !self.has_multi_line_strings()
            && !self.has_multi_polygons()
    }

    pub fn has_only_multi_points(&self) -> bool {
        !self.has_points()
            && !self.has_line_strings()
            && !self.has_polygons()
            && self.has_multi_points()
            && !self.has_multi_line_strings()
            && !self.has_multi_polygons()
    }

    pub fn has_only_multi_line_strings(&self) -> bool {
        !self.has_points()
            && !self.has_line_strings()
            && !self.has_polygons()
            && !self.has_multi_points()
            && self.has_multi_line_strings()
            && !self.has_multi_polygons()
    }

    pub fn has_only_multi_polygons(&self) -> bool {
        !self.has_points()
            && !self.has_line_strings()
            && !self.has_polygons()
            && !self.has_multi_points()
            && !self.has_multi_line_strings()
            && self.has_multi_polygons()
    }

    /// The number of bytes occupied by this array.
    pub fn num_bytes(&self) -> usize {
        self.buffer_lengths().num_bytes()
    }

    /// Slices this [`MixedGeometryArray`] in place.
    ///
    /// # Implementation
    ///
    /// This operation is `O(F)` where `F` is the number of fields.
    ///
    /// # Panic
    ///
    /// This function panics iff `offset + length > self.len()`.
    #[inline]
    pub fn slice(&self, offset: usize, length: usize) -> Self {
        assert!(
            offset + length <= self.len(),
            "offset + length may not exceed length of array"
        );
        Self {
            coord_type: self.coord_type,
            dim: self.dim,
            type_ids: self.type_ids.slice(offset, length),
            offsets: self.offsets.slice(offset, length),
            points: self.points.clone(),
            line_strings: self.line_strings.clone(),
            polygons: self.polygons.clone(),
            multi_points: self.multi_points.clone(),
            multi_line_strings: self.multi_line_strings.clone(),
            multi_polygons: self.multi_polygons.clone(),
            metadata: self.metadata.clone(),
            slice_offset: self.slice_offset + offset,
        }
    }

    pub fn to_coord_type(&self, coord_type: CoordType) -> Self {
        self.clone().into_coord_type(coord_type)
    }

    pub fn into_coord_type(self, coord_type: CoordType) -> Self {
        let metadata = self.metadata();
        Self::new(
            self.type_ids,
            self.offsets,
            Some(self.points.into_coord_type(coord_type)),
            Some(self.line_strings.into_coord_type(coord_type)),
            Some(self.polygons.into_coord_type(coord_type)),
            Some(self.multi_points.into_coord_type(coord_type)),
            Some(self.multi_line_strings.into_coord_type(coord_type)),
            Some(self.multi_polygons.into_coord_type(coord_type)),
            metadata,
        )
    }

    pub fn contained_types(&self) -> HashSet<NativeType> {
        let mut types = HashSet::new();
        if self.has_points() {
            types.insert(self.points.data_type());
        }
        if self.has_line_strings() {
            types.insert(self.line_strings.data_type());
        }
        if self.has_polygons() {
            types.insert(self.polygons.data_type());
        }
        if self.has_multi_points() {
            types.insert(self.multi_points.data_type());
        }
        if self.has_multi_line_strings() {
            types.insert(self.multi_line_strings.data_type());
        }
        if self.has_multi_polygons() {
            types.insert(self.multi_polygons.data_type());
        }

        types
    }
}

impl ArrayBase for MixedGeometryArray {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn storage_type(&self) -> DataType {
        mixed_data_type(self.coord_type, self.dim)
    }

    fn extension_field(&self) -> Arc<Field> {
        let name = "geometry";
        let nullable = true;
        let array_metadata = &self.metadata;
        let data_type = self.storage_type();

        // Note: this is currently copied from to_field_with_metadata
        let extension_name = self.extension_name();
        let mut metadata = HashMap::with_capacity(2);
        metadata.insert(
            EXTENSION_TYPE_NAME_KEY.to_string(),
            extension_name.to_string(),
        );
        if array_metadata.should_serialize() {
            metadata.insert(
                EXTENSION_TYPE_METADATA_KEY.to_string(),
                serde_json::to_string(array_metadata.as_ref()).unwrap(),
            );
        }
        Arc::new(Field::new(name, data_type, nullable).with_metadata(metadata))
    }

    fn extension_name(&self) -> &str {
        "geoarrow.geometry"
    }

    fn into_array_ref(self) -> ArrayRef {
        Arc::new(self.into_arrow())
    }

    fn to_array_ref(&self) -> ArrayRef {
        self.clone().into_array_ref()
    }

    fn metadata(&self) -> Arc<Metadata> {
        self.metadata.clone()
    }

    /// Returns the number of geometries in this array
    #[inline]
    fn len(&self) -> usize {
        // Note that `type_ids` is sliced as usual, and thus always has the correct length.
        self.type_ids.len()
    }

    /// Returns the optional validity.
    #[inline]
    fn nulls(&self) -> Option<&NullBuffer> {
        None
    }
}

impl NativeArray for MixedGeometryArray {
    fn data_type(&self) -> NativeType {
        // self.data_type
        todo!("mixed array does not have native type")
    }

    fn dimension(&self) -> Dimension {
        self.dim
    }

    fn coord_type(&self) -> CoordType {
        self.coord_type
    }

    fn to_coord_type(&self, coord_type: CoordType) -> Arc<dyn NativeArray> {
        Arc::new(self.clone().into_coord_type(coord_type))
    }

    fn with_metadata(&self, metadata: Arc<Metadata>) -> crate::trait_::NativeArrayRef {
        let mut arr = self.clone();
        arr.metadata = metadata;
        Arc::new(arr)
    }

    fn as_ref(&self) -> &dyn NativeArray {
        self
    }

    fn slice(&self, offset: usize, length: usize) -> Arc<dyn NativeArray> {
        Arc::new(self.slice(offset, length))
    }
}

impl GeometryArraySelfMethods for MixedGeometryArray {
    fn with_coords(self, _coords: crate::array::CoordBuffer) -> Self {
        todo!();
    }

    fn into_coord_type(self, _coord_type: CoordType) -> Self {
        todo!();
    }
}

impl NativeGeometryAccessor for MixedGeometryArray {
    unsafe fn value_as_geometry_unchecked(&self, index: usize) -> crate::scalar::Geometry {
        let type_id = self.type_ids[index];
        let offset = self.offsets[index] as usize;

        match type_id {
            1 => Geometry::Point(self.points.value(offset)),
            2 => Geometry::LineString(self.line_strings.value(offset)),
            3 => Geometry::Polygon(self.polygons.value(offset)),
            4 => Geometry::MultiPoint(self.multi_points.value(offset)),
            5 => Geometry::MultiLineString(self.multi_line_strings.value(offset)),
            6 => Geometry::MultiPolygon(self.multi_polygons.value(offset)),
            7 => {
                panic!("nested geometry collections not supported")
            }
            11 => Geometry::Point(self.points.value(offset)),
            12 => Geometry::LineString(self.line_strings.value(offset)),
            13 => Geometry::Polygon(self.polygons.value(offset)),
            14 => Geometry::MultiPoint(self.multi_points.value(offset)),
            15 => Geometry::MultiLineString(self.multi_line_strings.value(offset)),
            16 => Geometry::MultiPolygon(self.multi_polygons.value(offset)),
            17 => {
                panic!("nested geometry collections not supported")
            }
            _ => panic!("unknown type_id {}", type_id),
        }
    }
}

#[cfg(feature = "geos")]
impl<'a> crate::trait_::NativeGEOSGeometryAccessor<'a> for MixedGeometryArray {
    unsafe fn value_as_geometry_unchecked(
        &'a self,
        index: usize,
    ) -> std::result::Result<geos::Geometry, geos::Error> {
        let geom = NativeGeometryAccessor::value_as_geometry_unchecked(self, index);
        (&geom).try_into()
    }
}

impl<'a> ArrayAccessor<'a> for MixedGeometryArray {
    type Item = Geometry<'a>;
    type ItemGeo = geo::Geometry;

    unsafe fn value_unchecked(&'a self, index: usize) -> Self::Item {
        let type_id = self.type_ids[index];
        let offset = self.offsets[index] as usize;

        match type_id {
            1 => Geometry::Point(self.points.value(offset)),
            2 => Geometry::LineString(self.line_strings.value(offset)),
            3 => Geometry::Polygon(self.polygons.value(offset)),
            4 => Geometry::MultiPoint(self.multi_points.value(offset)),
            5 => Geometry::MultiLineString(self.multi_line_strings.value(offset)),
            6 => Geometry::MultiPolygon(self.multi_polygons.value(offset)),
            7 => {
                panic!("nested geometry collections not supported")
            }
            11 => Geometry::Point(self.points.value(offset)),
            12 => Geometry::LineString(self.line_strings.value(offset)),
            13 => Geometry::Polygon(self.polygons.value(offset)),
            14 => Geometry::MultiPoint(self.multi_points.value(offset)),
            15 => Geometry::MultiLineString(self.multi_line_strings.value(offset)),
            16 => Geometry::MultiPolygon(self.multi_polygons.value(offset)),
            17 => {
                panic!("nested geometry collections not supported")
            }
            _ => panic!("unknown type_id {}", type_id),
        }
    }
}

impl IntoArrow for MixedGeometryArray {
    type ArrowArray = UnionArray;

    fn into_arrow(self) -> Self::ArrowArray {
        let union_fields = match mixed_data_type(self.coord_type, self.dim) {
            DataType::Union(union_fields, _) => union_fields,
            _ => unreachable!(),
        };

        let child_arrays = vec![
            self.points.into_array_ref(),
            self.line_strings.into_array_ref(),
            self.polygons.into_array_ref(),
            self.multi_points.into_array_ref(),
            self.multi_line_strings.into_array_ref(),
            self.multi_polygons.into_array_ref(),
        ];

        UnionArray::try_new(
            union_fields,
            self.type_ids,
            Some(self.offsets),
            child_arrays,
        )
        .unwrap()
    }
}

impl TryFrom<(&UnionArray, Dimension)> for MixedGeometryArray {
    type Error = GeoArrowError;

    fn try_from((value, dim): (&UnionArray, Dimension)) -> std::result::Result<Self, Self::Error> {
        let mut points: Option<PointArray> = None;
        let mut line_strings: Option<LineStringArray> = None;
        let mut polygons: Option<PolygonArray> = None;
        let mut multi_points: Option<MultiPointArray> = None;
        let mut multi_line_strings: Option<MultiLineStringArray> = None;
        let mut multi_polygons: Option<MultiPolygonArray> = None;

        match value.data_type() {
            DataType::Union(fields, mode) => {
                if !matches!(mode, UnionMode::Dense) {
                    return Err(GeoArrowError::General("Expected dense union".to_string()));
                }

                for (type_id, _field) in fields.iter() {
                    let found_dimension = if type_id < 10 {
                        Dimension::XY
                    } else if type_id < 20 {
                        Dimension::XYZ
                    } else {
                        return Err(GeoArrowError::General(format!(
                            "Unsupported type_id: {}",
                            type_id
                        )));
                    };

                    if dim != found_dimension {
                        return Err(  GeoArrowError::General(format!("expected dimension: {:?}, found child array with dimension {:?} and type_id: {}", dim, found_dimension, type_id )));
                    }

                    match type_id {
                        1 | 11 => {
                            points = Some((value.child(type_id).as_ref(), dim).try_into().unwrap());
                        }
                        2 | 12 => {
                            line_strings =
                                Some((value.child(type_id).as_ref(), dim).try_into().unwrap());
                        }
                        3 | 13 => {
                            polygons =
                                Some((value.child(type_id).as_ref(), dim).try_into().unwrap());
                        }
                        4 | 14 => {
                            multi_points =
                                Some((value.child(type_id).as_ref(), dim).try_into().unwrap());
                        }
                        5 | 15 => {
                            multi_line_strings =
                                Some((value.child(type_id).as_ref(), dim).try_into().unwrap());
                        }
                        6 | 16 => {
                            multi_polygons =
                                Some((value.child(type_id).as_ref(), dim).try_into().unwrap());
                        }
                        _ => {
                            return Err(GeoArrowError::General(format!(
                                "Unexpected type_id {}",
                                type_id
                            )))
                        }
                    }
                }
            }
            _ => panic!("expected union type"),
        };

        let type_ids = value.type_ids().clone();
        // This is after checking for dense union
        let offsets = value.offsets().unwrap().clone();

        Ok(Self::new(
            type_ids,
            offsets,
            points,
            line_strings,
            polygons,
            multi_points,
            multi_line_strings,
            multi_polygons,
            Default::default(),
        ))
    }
}

impl TryFrom<(&dyn Array, Dimension)> for MixedGeometryArray {
    type Error = GeoArrowError;

    fn try_from((value, dim): (&dyn Array, Dimension)) -> Result<Self> {
        match value.data_type() {
            DataType::Union(_, _) => {
                let downcasted = value.as_any().downcast_ref::<UnionArray>().unwrap();
                (downcasted, dim).try_into()
            }
            _ => Err(GeoArrowError::General(format!(
                "Unexpected type: {:?}",
                value.data_type()
            ))),
        }
    }
}

// TODO:, thinking all geoarrow.geometry will go through primary dimensionless geometry array
impl TryFrom<(&dyn Array, &Field)> for MixedGeometryArray {
    type Error = GeoArrowError;

    fn try_from((arr, field): (&dyn Array, &Field)) -> Result<Self> {
        let geom_type = NativeType::try_from(field)?;
        let dim = geom_type
            .dimension()
            .ok_or(GeoArrowError::General("Expected dimension".to_string()))?;
        let mut arr: Self = (arr, dim).try_into()?;
        let metadata = Arc::new(Metadata::try_from(field)?);
        arr.data_type = arr.data_type.clone().with_metadata(metadata);
        Ok(arr)
    }
}

impl<G: GeometryTrait<T = f64>> TryFrom<(&[G], Dimension)> for MixedGeometryArray {
    type Error = GeoArrowError;

    fn try_from(geoms: (&[G], Dimension)) -> Result<Self> {
        let mut_arr: MixedGeometryBuilder = geoms.try_into()?;
        Ok(mut_arr.into())
    }
}

impl<G: GeometryTrait<T = f64>> TryFrom<(Vec<Option<G>>, Dimension)> for MixedGeometryArray {
    type Error = GeoArrowError;

    fn try_from(geoms: (Vec<Option<G>>, Dimension)) -> Result<Self> {
        let mut_arr: MixedGeometryBuilder = geoms.try_into()?;
        Ok(mut_arr.into())
    }
}

impl<O: OffsetSizeTrait> TryFrom<(WKBArray<O>, Dimension)> for MixedGeometryArray {
    type Error = GeoArrowError;

    fn try_from(value: (WKBArray<O>, Dimension)) -> Result<Self> {
        let mut_arr: MixedGeometryBuilder = value.try_into()?;
        Ok(mut_arr.into())
    }
}

impl From<PointArray> for MixedGeometryArray {
    fn from(value: PointArray) -> Self {
        let type_ids = match value.dimension() {
            Dimension::XY => vec![1; value.len()],
            Dimension::XYZ => vec![11; value.len()],
        };
        let metadata = value.metadata();
        Self::new(
            ScalarBuffer::from(type_ids),
            ScalarBuffer::from_iter(0..value.len() as i32),
            Some(value),
            None,
            None,
            None,
            None,
            None,
            metadata,
        )
    }
}

impl From<LineStringArray> for MixedGeometryArray {
    fn from(value: LineStringArray) -> Self {
        let type_ids = match value.dimension() {
            Dimension::XY => vec![2; value.len()],
            Dimension::XYZ => vec![12; value.len()],
        };
        let metadata = value.metadata();
        Self::new(
            ScalarBuffer::from(type_ids),
            ScalarBuffer::from_iter(0..value.len() as i32),
            None,
            Some(value),
            None,
            None,
            None,
            None,
            metadata,
        )
    }
}

impl From<PolygonArray> for MixedGeometryArray {
    fn from(value: PolygonArray) -> Self {
        let type_ids = match value.dimension() {
            Dimension::XY => vec![3; value.len()],
            Dimension::XYZ => vec![13; value.len()],
        };
        let metadata = value.metadata();
        Self::new(
            ScalarBuffer::from(type_ids),
            ScalarBuffer::from_iter(0..value.len() as i32),
            None,
            None,
            Some(value),
            None,
            None,
            None,
            metadata,
        )
    }
}

impl From<MultiPointArray> for MixedGeometryArray {
    fn from(value: MultiPointArray) -> Self {
        let type_ids = match value.dimension() {
            Dimension::XY => vec![4; value.len()],
            Dimension::XYZ => vec![14; value.len()],
        };
        let metadata = value.metadata();
        Self::new(
            ScalarBuffer::from(type_ids),
            ScalarBuffer::from_iter(0..value.len() as i32),
            None,
            None,
            None,
            Some(value),
            None,
            None,
            metadata,
        )
    }
}

impl From<MultiLineStringArray> for MixedGeometryArray {
    fn from(value: MultiLineStringArray) -> Self {
        let type_ids = match value.dimension() {
            Dimension::XY => vec![5; value.len()],
            Dimension::XYZ => vec![15; value.len()],
        };
        let metadata = value.metadata();
        Self::new(
            ScalarBuffer::from(type_ids),
            ScalarBuffer::from_iter(0..value.len() as i32),
            None,
            None,
            None,
            None,
            Some(value),
            None,
            metadata,
        )
    }
}

impl From<MultiPolygonArray> for MixedGeometryArray {
    fn from(value: MultiPolygonArray) -> Self {
        let type_ids = match value.dimension() {
            Dimension::XY => vec![6; value.len()],
            Dimension::XYZ => vec![16; value.len()],
        };
        let metadata = value.metadata();
        Self::new(
            ScalarBuffer::from(type_ids),
            ScalarBuffer::from_iter(0..value.len() as i32),
            None,
            None,
            None,
            None,
            None,
            Some(value),
            metadata,
        )
    }
}

impl TryFrom<GeometryCollectionArray> for MixedGeometryArray {
    type Error = GeoArrowError;

    fn try_from(value: GeometryCollectionArray) -> std::result::Result<Self, Self::Error> {
        if !can_downcast_multi(&value.geom_offsets) {
            return Err(GeoArrowError::General("Unable to cast".to_string()));
        }

        if value.null_count() > 0 {
            return Err(GeoArrowError::General(
                "Unable to cast with nulls".to_string(),
            ));
        }

        Ok(value.array)
    }
}

/// Default to an empty array
impl Default for MixedGeometryArray {
    fn default() -> Self {
        MixedGeometryBuilder::default().into()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::array::MixedGeometryArray;
    use crate::test::{linestring, multilinestring, multipoint, multipolygon, point, polygon};

    #[test]
    fn geo_roundtrip_accurate_points() {
        let geoms: Vec<geo::Geometry> = vec![
            geo::Geometry::Point(point::p0()),
            geo::Geometry::Point(point::p1()),
            geo::Geometry::Point(point::p2()),
        ];
        let arr: MixedGeometryArray = (geoms.as_slice(), Dimension::XY).try_into().unwrap();

        assert_eq!(
            arr.value_as_geo(0),
            geo::Geometry::MultiPoint(geo::MultiPoint(vec![point::p0()]))
        );
        assert_eq!(
            arr.value_as_geo(1),
            geo::Geometry::MultiPoint(geo::MultiPoint(vec![point::p1()]))
        );
        assert_eq!(
            arr.value_as_geo(2),
            geo::Geometry::MultiPoint(geo::MultiPoint(vec![point::p2()]))
        );
    }

    #[test]
    fn geo_roundtrip_accurate_all() {
        let geoms: Vec<geo::Geometry> = vec![
            geo::Geometry::Point(point::p0()),
            geo::Geometry::LineString(linestring::ls0()),
            geo::Geometry::Polygon(polygon::p0()),
            geo::Geometry::MultiPoint(multipoint::mp0()),
            geo::Geometry::MultiLineString(multilinestring::ml0()),
            geo::Geometry::MultiPolygon(multipolygon::mp0()),
        ];
        let arr: MixedGeometryArray = (geoms.as_slice(), Dimension::XY).try_into().unwrap();

        assert_eq!(
            arr.value_as_geo(0),
            geo::Geometry::MultiPoint(geo::MultiPoint(vec![point::p0()]))
        );
        assert_eq!(
            arr.value_as_geo(1),
            geo::Geometry::MultiLineString(geo::MultiLineString(vec![linestring::ls0()]))
        );
        assert_eq!(
            arr.value_as_geo(2),
            geo::Geometry::MultiPolygon(geo::MultiPolygon(vec![polygon::p0()]))
        );
        assert_eq!(arr.value_as_geo(3), geoms[3]);
        assert_eq!(arr.value_as_geo(4), geoms[4]);
        assert_eq!(arr.value_as_geo(5), geoms[5]);
    }

    #[test]
    fn arrow_roundtrip() {
        let geoms: Vec<geo::Geometry> = vec![
            geo::Geometry::Point(point::p0()),
            geo::Geometry::LineString(linestring::ls0()),
            geo::Geometry::Polygon(polygon::p0()),
            geo::Geometry::MultiPoint(multipoint::mp0()),
            geo::Geometry::MultiLineString(multilinestring::ml0()),
            geo::Geometry::MultiPolygon(multipolygon::mp0()),
        ];
        let arr: MixedGeometryArray = (geoms.as_slice(), Dimension::XY).try_into().unwrap();

        // Round trip to/from arrow-rs
        let arrow_array = arr.into_arrow();
        let round_trip_arr: MixedGeometryArray = (&arrow_array, Dimension::XY).try_into().unwrap();

        assert_eq!(
            round_trip_arr.value_as_geo(0),
            geo::Geometry::MultiPoint(geo::MultiPoint(vec![point::p0()]))
        );
        assert_eq!(
            round_trip_arr.value_as_geo(1),
            geo::Geometry::MultiLineString(geo::MultiLineString(vec![linestring::ls0()]))
        );
        assert_eq!(
            round_trip_arr.value_as_geo(2),
            geo::Geometry::MultiPolygon(geo::MultiPolygon(vec![polygon::p0()]))
        );
        assert_eq!(round_trip_arr.value_as_geo(3), geoms[3]);
        assert_eq!(round_trip_arr.value_as_geo(4), geoms[4]);
        assert_eq!(round_trip_arr.value_as_geo(5), geoms[5]);
    }

    #[test]
    fn arrow_roundtrip_not_all_types() {
        let geoms: Vec<geo::Geometry> = vec![
            geo::Geometry::MultiPoint(multipoint::mp0()),
            geo::Geometry::MultiLineString(multilinestring::ml0()),
            geo::Geometry::MultiPolygon(multipolygon::mp0()),
        ];
        let arr: MixedGeometryArray = (geoms.as_slice(), Dimension::XY).try_into().unwrap();

        // Round trip to/from arrow-rs
        let arrow_array = arr.into_arrow();
        let round_trip_arr: MixedGeometryArray = (&arrow_array, Dimension::XY).try_into().unwrap();

        assert_eq!(round_trip_arr.value_as_geo(0), geoms[0]);
        assert_eq!(round_trip_arr.value_as_geo(1), geoms[1]);
        assert_eq!(round_trip_arr.value_as_geo(2), geoms[2]);
    }

    #[test]
    fn arrow_roundtrip_not_all_types2() {
        let geoms: Vec<geo::Geometry> = vec![
            geo::Geometry::MultiPoint(multipoint::mp0()),
            geo::Geometry::MultiPolygon(multipolygon::mp0()),
        ];
        let arr: MixedGeometryArray = (geoms.as_slice(), Dimension::XY).try_into().unwrap();

        // Round trip to/from arrow-rs
        let arrow_array = arr.into_arrow();
        let round_trip_arr: MixedGeometryArray = (&arrow_array, Dimension::XY).try_into().unwrap();

        assert_eq!(round_trip_arr.value_as_geo(0), geoms[0]);
        assert_eq!(round_trip_arr.value_as_geo(1), geoms[1]);
    }
}
