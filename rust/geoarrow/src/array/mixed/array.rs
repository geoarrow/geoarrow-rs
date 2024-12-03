use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::{Array, OffsetSizeTrait, UnionArray};
use arrow_buffer::{NullBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, UnionMode};

use crate::algorithm::native::downcast::can_downcast_multi;
use crate::array::metadata::ArrayMetadata;
use crate::array::mixed::builder::MixedGeometryBuilder;
use crate::array::mixed::MixedCapacity;
use crate::array::{
    CoordType, GeometryCollectionArray, LineStringArray, MultiLineStringArray, MultiPointArray,
    MultiPolygonArray, PointArray, PolygonArray, WKBArray,
};
use crate::datatypes::{Dimension, NativeType};
use crate::error::{GeoArrowError, Result};
use crate::scalar::Geometry;
use crate::trait_::{ArrayAccessor, GeometryArraySelfMethods, IntoArrow, NativeGeometryAccessor};
use crate::{ArrayBase, NativeArray};
use geo_traits::GeometryTrait;

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
    /// Always NativeType::Mixed
    data_type: NativeType,

    pub(crate) metadata: Arc<ArrayMetadata>,

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

    /// An offset used for slicing into this array. The offset will be 0 if the array has not been
    /// sliced.
    ///
    /// In order to slice this array efficiently (and zero-cost) we can't slice the underlying
    /// fields directly. If this were always a _sparse_ union array, we could! We could then always
    /// slice from offset to length of each underlying array. But we're under the assumption that
    /// most or all of the time we have a dense union array, where the `offsets` buffer is defined.
    /// In that case, to know how to slice each underlying array, we'd have to walk the `type_ids`
    /// and `offsets` arrays (in O(N) time) to figure out how to slice the underlying arrays.
    ///
    /// Instead, we store the slice offset.
    ///
    /// Note that this offset is only for slicing into the **fields**, i.e. the geometry arrays.
    /// The `type_ids` and `offsets` arrays are sliced as usual.
    ///
    /// TODO: when exporting this array, export to arrow2 and then slice from scratch because we
    /// can't set the `offset` in a UnionArray constructor
    pub(crate) slice_offset: usize,
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
        points: PointArray,
        line_strings: LineStringArray,
        polygons: PolygonArray,
        multi_points: MultiPointArray,
        multi_line_strings: MultiLineStringArray,
        multi_polygons: MultiPolygonArray,
        metadata: Arc<ArrayMetadata>,
    ) -> Self {
        let mut coord_types = HashSet::new();
        coord_types.insert(points.coord_type());
        coord_types.insert(line_strings.coord_type());
        coord_types.insert(polygons.coord_type());
        coord_types.insert(multi_points.coord_type());
        coord_types.insert(multi_line_strings.coord_type());
        coord_types.insert(multi_polygons.coord_type());
        assert_eq!(coord_types.len(), 1);

        let coord_type = coord_types.into_iter().next().unwrap();

        let mut dimensions = HashSet::new();
        dimensions.insert(points.dimension());
        dimensions.insert(line_strings.dimension());
        dimensions.insert(polygons.dimension());
        dimensions.insert(multi_points.dimension());
        dimensions.insert(multi_line_strings.dimension());
        dimensions.insert(multi_polygons.dimension());
        assert_eq!(dimensions.len(), 1);

        let dim = dimensions.into_iter().next().unwrap();

        let data_type = NativeType::Mixed(coord_type, dim);

        Self {
            data_type,
            type_ids,
            offsets,
            points,
            line_strings,
            polygons,
            multi_points,
            multi_line_strings,
            multi_polygons,
            slice_offset: 0,
            metadata,
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

    pub fn has_points(&self) -> bool {
        !self.points.is_empty()
    }

    pub fn has_line_strings(&self) -> bool {
        !self.line_strings.is_empty()
    }

    pub fn has_polygons(&self) -> bool {
        !self.polygons.is_empty()
    }

    pub fn has_multi_points(&self) -> bool {
        !self.multi_points.is_empty()
    }

    pub fn has_multi_line_strings(&self) -> bool {
        !self.multi_line_strings.is_empty()
    }

    pub fn has_multi_polygons(&self) -> bool {
        !self.multi_polygons.is_empty()
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
            data_type: self.data_type,
            type_ids: self.type_ids.slice(offset, length),
            offsets: self.offsets.slice(offset, length),
            points: self.points.clone(),
            line_strings: self.line_strings.clone(),
            polygons: self.polygons.clone(),
            multi_points: self.multi_points.clone(),
            multi_line_strings: self.multi_line_strings.clone(),
            multi_polygons: self.multi_polygons.clone(),
            slice_offset: self.slice_offset + offset,
            metadata: self.metadata.clone(),
        }
    }

    pub fn to_coord_type(&self, coord_type: CoordType) -> Self {
        self.clone().into_coord_type(coord_type)
    }

    pub fn into_coord_type(self, coord_type: CoordType) -> Self {
        Self::new(
            self.type_ids,
            self.offsets,
            self.points.into_coord_type(coord_type),
            self.line_strings.into_coord_type(coord_type),
            self.polygons.into_coord_type(coord_type),
            self.multi_points.into_coord_type(coord_type),
            self.multi_line_strings.into_coord_type(coord_type),
            self.multi_polygons.into_coord_type(coord_type),
            self.metadata,
        )
    }
}

impl ArrayBase for MixedGeometryArray {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn storage_type(&self) -> DataType {
        self.data_type.to_data_type()
    }

    fn extension_field(&self) -> Arc<Field> {
        Arc::new(
            self.data_type
                .to_field_with_metadata("geometry", true, &self.metadata),
        )
    }

    fn extension_name(&self) -> &str {
        self.data_type.extension_name()
    }

    fn into_array_ref(self) -> Arc<dyn Array> {
        Arc::new(self.into_arrow())
    }

    fn to_array_ref(&self) -> arrow_array::ArrayRef {
        self.clone().into_array_ref()
    }

    fn metadata(&self) -> Arc<ArrayMetadata> {
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
        self.data_type
    }

    fn coord_type(&self) -> crate::array::CoordType {
        self.data_type.coord_type()
    }

    fn to_coord_type(&self, coord_type: CoordType) -> Arc<dyn NativeArray> {
        Arc::new(self.clone().into_coord_type(coord_type))
    }

    fn with_metadata(&self, metadata: Arc<ArrayMetadata>) -> crate::trait_::NativeArrayRef {
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

    fn into_coord_type(self, _coord_type: crate::array::CoordType) -> Self {
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
        let union_fields = match self.data_type.to_data_type() {
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

impl TryFrom<&UnionArray> for MixedGeometryArray {
    type Error = GeoArrowError;

    fn try_from(value: &UnionArray) -> std::result::Result<Self, Self::Error> {
        let mut points: Vec<PointArray> = vec![];
        let mut line_strings: Vec<LineStringArray> = vec![];
        let mut polygons: Vec<PolygonArray> = vec![];
        let mut multi_points: Vec<MultiPointArray> = vec![];
        let mut multi_line_strings: Vec<MultiLineStringArray> = vec![];
        let mut multi_polygons: Vec<MultiPolygonArray> = vec![];
        match value.data_type() {
            DataType::Union(fields, mode) => {
                if !matches!(mode, UnionMode::Dense) {
                    return Err(GeoArrowError::General("Expected dense union".to_string()));
                }

                for (type_id, _field) in fields.iter() {
                    let dimension = if type_id < 10 {
                        Dimension::XY
                    } else if type_id < 20 {
                        Dimension::XYZ
                    } else {
                        return Err(GeoArrowError::General(format!(
                            "Unsupported type_id: {}",
                            type_id
                        )));
                    };

                    match type_id {
                        1 | 11 => {
                            points.push(
                                (value.child(type_id).as_ref(), dimension)
                                    .try_into()
                                    .unwrap(),
                            );
                        }
                        2 | 12 => {
                            line_strings.push(
                                (value.child(type_id).as_ref(), dimension)
                                    .try_into()
                                    .unwrap(),
                            );
                        }
                        3 | 13 => {
                            polygons.push(
                                (value.child(type_id).as_ref(), dimension)
                                    .try_into()
                                    .unwrap(),
                            );
                        }
                        4 | 14 => {
                            multi_points.push(
                                (value.child(type_id).as_ref(), dimension)
                                    .try_into()
                                    .unwrap(),
                            );
                        }
                        5 | 15 => {
                            multi_line_strings.push(
                                (value.child(type_id).as_ref(), dimension)
                                    .try_into()
                                    .unwrap(),
                            );
                        }
                        6 | 16 => {
                            multi_polygons.push(
                                (value.child(type_id).as_ref(), dimension)
                                    .try_into()
                                    .unwrap(),
                            );
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

        // TODO: make nicer errors. We don't currently allow a mixed geometry array with multiple
        // dimensions of underlying geometries.
        assert!(points.len() <= 1);
        assert!(line_strings.len() <= 1);
        assert!(polygons.len() <= 1);
        assert!(multi_points.len() <= 1);
        assert!(multi_line_strings.len() <= 1);
        assert!(multi_polygons.len() <= 1);

        Ok(Self::new(
            type_ids,
            offsets,
            points.first().cloned().unwrap_or_default(),
            line_strings.first().cloned().unwrap_or_default(),
            polygons.first().cloned().unwrap_or_default(),
            multi_points.first().cloned().unwrap_or_default(),
            multi_line_strings.first().cloned().unwrap_or_default(),
            multi_polygons.first().cloned().unwrap_or_default(),
            Default::default(),
        ))
    }
}

impl TryFrom<&dyn Array> for MixedGeometryArray {
    type Error = GeoArrowError;

    fn try_from(value: &dyn Array) -> Result<Self> {
        match value.data_type() {
            DataType::Union(_, _) => {
                let downcasted = value.as_any().downcast_ref::<UnionArray>().unwrap();
                downcasted.try_into()
            }
            _ => Err(GeoArrowError::General(format!(
                "Unexpected type: {:?}",
                value.data_type()
            ))),
        }
    }
}

impl TryFrom<(&dyn Array, &Field)> for MixedGeometryArray {
    type Error = GeoArrowError;

    fn try_from((arr, field): (&dyn Array, &Field)) -> Result<Self> {
        let mut arr: Self = arr.try_into()?;
        arr.metadata = Arc::new(ArrayMetadata::try_from(field)?);
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
        let metadata = value.metadata.clone();
        Self::new(
            ScalarBuffer::from(type_ids),
            ScalarBuffer::from_iter(0..value.len() as i32),
            value,
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
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
        let metadata = value.metadata.clone();
        Self::new(
            ScalarBuffer::from(type_ids),
            ScalarBuffer::from_iter(0..value.len() as i32),
            Default::default(),
            value,
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
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
        let metadata = value.metadata.clone();
        Self::new(
            ScalarBuffer::from(type_ids),
            ScalarBuffer::from_iter(0..value.len() as i32),
            Default::default(),
            Default::default(),
            value,
            Default::default(),
            Default::default(),
            Default::default(),
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
        let metadata = value.metadata.clone();
        Self::new(
            ScalarBuffer::from(type_ids),
            ScalarBuffer::from_iter(0..value.len() as i32),
            Default::default(),
            Default::default(),
            Default::default(),
            value,
            Default::default(),
            Default::default(),
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
        let metadata = value.metadata.clone();
        Self::new(
            ScalarBuffer::from(type_ids),
            ScalarBuffer::from_iter(0..value.len() as i32),
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
            value,
            Default::default(),
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
        let metadata = value.metadata.clone();
        Self::new(
            ScalarBuffer::from(type_ids),
            ScalarBuffer::from_iter(0..value.len() as i32),
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
            value,
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
        let round_trip_arr: MixedGeometryArray = (&arrow_array).try_into().unwrap();

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
        let round_trip_arr: MixedGeometryArray = (&arrow_array).try_into().unwrap();

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
        let round_trip_arr: MixedGeometryArray = (&arrow_array).try_into().unwrap();

        assert_eq!(round_trip_arr.value_as_geo(0), geoms[0]);
        assert_eq!(round_trip_arr.value_as_geo(1), geoms[1]);
    }
}
