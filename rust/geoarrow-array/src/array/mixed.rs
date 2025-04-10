use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, OffsetSizeTrait, UnionArray};
use arrow_buffer::ScalarBuffer;
use arrow_schema::{DataType, UnionMode};
use geoarrow_schema::{
    CoordType, Dimension, GeometryCollectionType, LineStringType, Metadata, MultiLineStringType,
    MultiPointType, MultiPolygonType, PointType, PolygonType,
};

use crate::ArrayAccessor;
use crate::array::{
    LineStringArray, MultiLineStringArray, MultiPointArray, MultiPolygonArray, PointArray,
    PolygonArray, WkbArray,
};
use crate::builder::{
    LineStringBuilder, MixedGeometryBuilder, MultiLineStringBuilder, MultiPointBuilder,
    MultiPolygonBuilder, PointBuilder, PolygonBuilder,
};
use crate::capacity::MixedCapacity;
use crate::datatypes::GeoArrowType;
use crate::error::{GeoArrowError, Result};
use crate::scalar::Geometry;
use crate::trait_::GeoArrowArray;

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
    pub(crate) coord_type: CoordType,
    pub(crate) dim: Dimension,

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
            coord_types.insert(points.data_type.coord_type());
        }
        if let Some(line_strings) = &line_strings {
            coord_types.insert(line_strings.data_type.coord_type());
        }
        if let Some(polygons) = &polygons {
            coord_types.insert(polygons.data_type.coord_type());
        }
        if let Some(multi_points) = &multi_points {
            coord_types.insert(multi_points.data_type.coord_type());
        }
        if let Some(multi_line_strings) = &multi_line_strings {
            coord_types.insert(multi_line_strings.data_type.coord_type());
        }
        if let Some(multi_polygons) = &multi_polygons {
            coord_types.insert(multi_polygons.data_type.coord_type());
        }
        assert!(coord_types.len() <= 1);
        let coord_type = coord_types
            .into_iter()
            .next()
            .unwrap_or(CoordType::Interleaved);

        let mut dimensions = HashSet::new();
        if let Some(points) = &points {
            dimensions.insert(points.data_type.dimension());
        }
        if let Some(line_strings) = &line_strings {
            dimensions.insert(line_strings.data_type.dimension());
        }
        if let Some(polygons) = &polygons {
            dimensions.insert(polygons.data_type.dimension());
        }
        if let Some(multi_points) = &multi_points {
            dimensions.insert(multi_points.data_type.dimension());
        }
        if let Some(multi_line_strings) = &multi_line_strings {
            dimensions.insert(multi_line_strings.data_type.dimension());
        }
        if let Some(multi_polygons) = &multi_polygons {
            dimensions.insert(multi_polygons.data_type.dimension());
        }
        assert_eq!(dimensions.len(), 1);
        let dim = dimensions.into_iter().next().unwrap();

        Self {
            coord_type,
            dim,
            type_ids,
            offsets,
            points: points.unwrap_or(
                PointBuilder::new(PointType::new(coord_type, dim, Default::default())).finish(),
            ),
            line_strings: line_strings.unwrap_or(
                LineStringBuilder::new(LineStringType::new(coord_type, dim, Default::default()))
                    .finish(),
            ),
            polygons: polygons.unwrap_or(
                PolygonBuilder::new(PolygonType::new(coord_type, dim, Default::default())).finish(),
            ),
            multi_points: multi_points.unwrap_or(
                MultiPointBuilder::new(MultiPointType::new(coord_type, dim, Default::default()))
                    .finish(),
            ),
            multi_line_strings: multi_line_strings.unwrap_or(
                MultiLineStringBuilder::new(MultiLineStringType::new(
                    coord_type,
                    dim,
                    Default::default(),
                ))
                .finish(),
            ),
            multi_polygons: multi_polygons.unwrap_or(
                MultiPolygonBuilder::new(MultiPolygonType::new(
                    coord_type,
                    dim,
                    Default::default(),
                ))
                .finish(),
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

    pub fn contained_types(&self) -> HashSet<GeoArrowType> {
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

    pub(crate) fn storage_type(&self) -> DataType {
        match GeometryCollectionType::new(self.coord_type, self.dim, Default::default()).data_type()
        {
            DataType::List(inner_field) => inner_field.data_type().clone(),
            _ => unreachable!(),
        }
    }

    pub(crate) fn into_array_ref(self) -> ArrayRef {
        Arc::new(UnionArray::from(self))
    }

    /// Returns the number of geometries in this array
    #[inline]
    fn len(&self) -> usize {
        // Note that `type_ids` is sliced as usual, and thus always has the correct length.
        self.type_ids.len()
    }

    // Note: this is copied from ArrayAccessor because MixedGeometryArray doesn't implement
    // GeoArrowArray
    pub(crate) unsafe fn value_unchecked(&self, index: usize) -> Geometry {
        let type_id = self.type_ids[index];
        let offset = self.offsets[index] as usize;

        let expect_msg = "native geometry value access should never error";
        match type_id {
            1 => Geometry::Point(self.points.value(offset).expect(expect_msg)),
            2 => Geometry::LineString(self.line_strings.value(offset).expect(expect_msg)),
            3 => Geometry::Polygon(self.polygons.value(offset).expect(expect_msg)),
            4 => Geometry::MultiPoint(self.multi_points.value(offset).expect(expect_msg)),
            5 => {
                Geometry::MultiLineString(self.multi_line_strings.value(offset).expect(expect_msg))
            }
            6 => Geometry::MultiPolygon(self.multi_polygons.value(offset).expect(expect_msg)),
            7 => {
                panic!("nested geometry collections not supported")
            }
            11 => Geometry::Point(self.points.value(offset).expect(expect_msg)),
            12 => Geometry::LineString(self.line_strings.value(offset).expect(expect_msg)),
            13 => Geometry::Polygon(self.polygons.value(offset).expect(expect_msg)),
            14 => Geometry::MultiPoint(self.multi_points.value(offset).expect(expect_msg)),
            15 => {
                Geometry::MultiLineString(self.multi_line_strings.value(offset).expect(expect_msg))
            }
            16 => Geometry::MultiPolygon(self.multi_polygons.value(offset).expect(expect_msg)),
            17 => {
                panic!("nested geometry collections not supported")
            }
            _ => panic!("unknown type_id {}", type_id),
        }
    }

    // Note: this is copied from ArrayAccessor because MixedGeometryArray doesn't implement
    // GeoArrowArray
    pub(crate) fn value(&self, index: usize) -> Geometry<'_> {
        assert!(index <= self.len());
        unsafe { self.value_unchecked(index) }
    }
}

impl From<MixedGeometryArray> for UnionArray {
    fn from(value: MixedGeometryArray) -> Self {
        let union_fields = match value.storage_type() {
            DataType::Union(union_fields, _) => union_fields,
            _ => unreachable!(),
        };

        let child_arrays = vec![
            value.points.into_array_ref(),
            value.line_strings.into_array_ref(),
            value.polygons.into_array_ref(),
            value.multi_points.into_array_ref(),
            value.multi_line_strings.into_array_ref(),
            value.multi_polygons.into_array_ref(),
        ];

        UnionArray::try_new(
            union_fields,
            value.type_ids,
            Some(value.offsets),
            child_arrays,
        )
        .unwrap()
    }
}

impl TryFrom<(&UnionArray, Dimension, CoordType)> for MixedGeometryArray {
    type Error = GeoArrowError;

    fn try_from(
        (value, dim, coord_type): (&UnionArray, Dimension, CoordType),
    ) -> std::result::Result<Self, Self::Error> {
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
                        return Err(GeoArrowError::General(format!(
                            "expected dimension: {:?}, found child array with dimension {:?} and type_id: {}",
                            dim, found_dimension, type_id
                        )));
                    }

                    match type_id {
                        1 | 11 => {
                            points = Some(
                                (
                                    value.child(type_id).as_ref(),
                                    PointType::new(coord_type, dim, Default::default()),
                                )
                                    .try_into()
                                    .unwrap(),
                            );
                        }
                        2 | 12 => {
                            line_strings = Some(
                                (
                                    value.child(type_id).as_ref(),
                                    LineStringType::new(coord_type, dim, Default::default()),
                                )
                                    .try_into()
                                    .unwrap(),
                            );
                        }
                        3 | 13 => {
                            polygons = Some(
                                (
                                    value.child(type_id).as_ref(),
                                    PolygonType::new(coord_type, dim, Default::default()),
                                )
                                    .try_into()
                                    .unwrap(),
                            );
                        }
                        4 | 14 => {
                            multi_points = Some(
                                (
                                    value.child(type_id).as_ref(),
                                    MultiPointType::new(coord_type, dim, Default::default()),
                                )
                                    .try_into()
                                    .unwrap(),
                            );
                        }
                        5 | 15 => {
                            multi_line_strings = Some(
                                (
                                    value.child(type_id).as_ref(),
                                    MultiLineStringType::new(coord_type, dim, Default::default()),
                                )
                                    .try_into()
                                    .unwrap(),
                            );
                        }
                        6 | 16 => {
                            multi_polygons = Some(
                                (
                                    value.child(type_id).as_ref(),
                                    MultiPolygonType::new(coord_type, dim, Default::default()),
                                )
                                    .try_into()
                                    .unwrap(),
                            );
                        }
                        _ => {
                            return Err(GeoArrowError::General(format!(
                                "Unexpected type_id {}",
                                type_id
                            )));
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
            // Mixed array is only used inside of GeometryCollectionArray, and this array does not
            // hold its own metadata
            Default::default(),
        ))
    }
}

impl TryFrom<(&dyn Array, Dimension, CoordType)> for MixedGeometryArray {
    type Error = GeoArrowError;

    fn try_from((value, dim, coord_type): (&dyn Array, Dimension, CoordType)) -> Result<Self> {
        match value.data_type() {
            DataType::Union(_, _) => (value.as_union(), dim, coord_type).try_into(),
            _ => Err(GeoArrowError::General(format!(
                "Unexpected type: {:?}",
                value.data_type()
            ))),
        }
    }
}

impl<O: OffsetSizeTrait> TryFrom<(WkbArray<O>, Dimension)> for MixedGeometryArray {
    type Error = GeoArrowError;

    fn try_from(value: (WkbArray<O>, Dimension)) -> Result<Self> {
        let mut_arr: MixedGeometryBuilder = value.try_into()?;
        Ok(mut_arr.finish())
    }
}
