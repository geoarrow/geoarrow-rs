use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, OffsetSizeTrait, UnionArray};
use arrow_buffer::{NullBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, UnionMode};

use crate::array::geometry::GeometryBuilder;
use crate::array::geometry::GeometryCapacity;
use crate::array::metadata::ArrayMetadata;
use crate::array::*;
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
pub struct GeometryArray {
    /// Always NativeType::Unknown
    data_type: NativeType,

    pub(crate) metadata: Arc<ArrayMetadata>,

    /// Invariant: every item in `type_ids` is `> 0 && < fields.len()` if `type_ids` are not
    /// provided. If `type_ids` exist in the NativeType, then every item in `type_ids` is `> 0 && `
    pub(crate) type_ids: ScalarBuffer<i8>,

    /// Invariant: `offsets.len() == type_ids.len()`
    pub(crate) offsets: ScalarBuffer<i32>,

    // In the future we'll additionally have xym, xyzm array variants.
    pub(crate) point_xy: PointArray,
    pub(crate) line_string_xy: LineStringArray,
    pub(crate) polygon_xy: PolygonArray,
    pub(crate) mpoint_xy: MultiPointArray,
    pub(crate) mline_string_xy: MultiLineStringArray,
    pub(crate) mpolygon_xy: MultiPolygonArray,
    pub(crate) gc_xy: GeometryCollectionArray,

    pub(crate) point_xyz: PointArray,
    pub(crate) line_string_xyz: LineStringArray,
    pub(crate) polygon_xyz: PolygonArray,
    pub(crate) mpoint_xyz: MultiPointArray,
    pub(crate) mline_string_xyz: MultiLineStringArray,
    pub(crate) mpolygon_xyz: MultiPolygonArray,
    pub(crate) gc_xyz: GeometryCollectionArray,
}

impl GeometryArray {
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
        point_xy: Option<PointArray>,
        line_string_xy: Option<LineStringArray>,
        polygon_xy: Option<PolygonArray>,
        mpoint_xy: Option<MultiPointArray>,
        mline_string_xy: Option<MultiLineStringArray>,
        mpolygon_xy: Option<MultiPolygonArray>,
        gc_xy: Option<GeometryCollectionArray>,
        point_xyz: Option<PointArray>,
        line_string_xyz: Option<LineStringArray>,
        polygon_xyz: Option<PolygonArray>,
        mpoint_xyz: Option<MultiPointArray>,
        mline_string_xyz: Option<MultiLineStringArray>,
        mpolygon_xyz: Option<MultiPolygonArray>,
        gc_xyz: Option<GeometryCollectionArray>,
        metadata: Arc<ArrayMetadata>,
    ) -> Self {
        let mut coord_types = HashSet::new();
        if let Some(point_xy) = &point_xy {
            coord_types.insert(point_xy.coord_type());
        }
        if let Some(line_string_xy) = &line_string_xy {
            coord_types.insert(line_string_xy.coord_type());
        }
        if let Some(polygon_xy) = &polygon_xy {
            coord_types.insert(polygon_xy.coord_type());
        }
        if let Some(mpoint_xy) = &mpoint_xy {
            coord_types.insert(mpoint_xy.coord_type());
        }
        if let Some(mline_string_xy) = &mline_string_xy {
            coord_types.insert(mline_string_xy.coord_type());
        }
        if let Some(mpolygon_xy) = &mpolygon_xy {
            coord_types.insert(mpolygon_xy.coord_type());
        }
        if let Some(gc_xy) = &gc_xy {
            coord_types.insert(gc_xy.coord_type());
        }

        if let Some(point_xyz) = &point_xyz {
            coord_types.insert(point_xyz.coord_type());
        }
        if let Some(line_string_xyz) = &line_string_xyz {
            coord_types.insert(line_string_xyz.coord_type());
        }
        if let Some(polygon_xyz) = &polygon_xyz {
            coord_types.insert(polygon_xyz.coord_type());
        }
        if let Some(mpoint_xyz) = &mpoint_xyz {
            coord_types.insert(mpoint_xyz.coord_type());
        }
        if let Some(mline_string_xyz) = &mline_string_xyz {
            coord_types.insert(mline_string_xyz.coord_type());
        }
        if let Some(mpolygon_xyz) = &mpolygon_xyz {
            coord_types.insert(mpolygon_xyz.coord_type());
        }
        if let Some(gc_xyz) = &gc_xyz {
            coord_types.insert(gc_xyz.coord_type());
        }
        assert!(coord_types.len() <= 1);

        let coord_type = coord_types.into_iter().next().unwrap_or_default();

        let data_type = NativeType::Geometry(coord_type);

        use Dimension::*;
        Self {
            data_type,
            type_ids,
            offsets,
            point_xy: point_xy.unwrap_or(
                PointBuilder::new_with_options(XY, coord_type, Default::default()).finish(),
            ),
            line_string_xy: line_string_xy.unwrap_or(
                LineStringBuilder::new_with_options(XY, coord_type, Default::default()).finish(),
            ),
            polygon_xy: polygon_xy.unwrap_or(
                PolygonBuilder::new_with_options(XY, coord_type, Default::default()).finish(),
            ),
            mpoint_xy: mpoint_xy.unwrap_or(
                MultiPointBuilder::new_with_options(XY, coord_type, Default::default()).finish(),
            ),
            mline_string_xy: mline_string_xy.unwrap_or(
                MultiLineStringBuilder::new_with_options(XY, coord_type, Default::default())
                    .finish(),
            ),
            mpolygon_xy: mpolygon_xy.unwrap_or(
                MultiPolygonBuilder::new_with_options(XY, coord_type, Default::default()).finish(),
            ),
            gc_xy: gc_xy.unwrap_or(
                GeometryCollectionBuilder::new_with_options(
                    XY,
                    coord_type,
                    Default::default(),
                    false,
                )
                .finish(),
            ),
            point_xyz: point_xyz.unwrap_or(
                PointBuilder::new_with_options(XYZ, coord_type, Default::default()).finish(),
            ),
            line_string_xyz: line_string_xyz.unwrap_or(
                LineStringBuilder::new_with_options(XYZ, coord_type, Default::default()).finish(),
            ),
            polygon_xyz: polygon_xyz.unwrap_or(
                PolygonBuilder::new_with_options(XYZ, coord_type, Default::default()).finish(),
            ),
            mpoint_xyz: mpoint_xyz.unwrap_or(
                MultiPointBuilder::new_with_options(XYZ, coord_type, Default::default()).finish(),
            ),
            mline_string_xyz: mline_string_xyz.unwrap_or(
                MultiLineStringBuilder::new_with_options(XYZ, coord_type, Default::default())
                    .finish(),
            ),
            mpolygon_xyz: mpolygon_xyz.unwrap_or(
                MultiPolygonBuilder::new_with_options(XYZ, coord_type, Default::default()).finish(),
            ),
            gc_xyz: gc_xyz.unwrap_or(
                GeometryCollectionBuilder::new_with_options(
                    XYZ,
                    coord_type,
                    Default::default(),
                    false,
                )
                .finish(),
            ),
            metadata,
        }
    }

    /// The lengths of each buffer contained in this array.
    pub fn buffer_lengths(&self) -> GeometryCapacity {
        GeometryCapacity::new(
            0,
            self.point_xy.buffer_lengths(),
            self.line_string_xy.buffer_lengths(),
            self.polygon_xy.buffer_lengths(),
            self.mpoint_xy.buffer_lengths(),
            self.mline_string_xy.buffer_lengths(),
            self.mpolygon_xy.buffer_lengths(),
            self.gc_xy.buffer_lengths(),
            self.point_xyz.buffer_lengths(),
            self.line_string_xyz.buffer_lengths(),
            self.polygon_xyz.buffer_lengths(),
            self.mpoint_xyz.buffer_lengths(),
            self.mline_string_xyz.buffer_lengths(),
            self.mpolygon_xyz.buffer_lengths(),
            self.gc_xyz.buffer_lengths(),
            false,
        )
    }

    // TODO: handle slicing
    pub(crate) fn has_points(&self, dim: Dimension) -> bool {
        match dim {
            Dimension::XY => !self.point_xy.is_empty(),
            Dimension::XYZ => !self.point_xyz.is_empty(),
        }
    }

    pub(crate) fn has_line_strings(&self, dim: Dimension) -> bool {
        match dim {
            Dimension::XY => !self.line_string_xy.is_empty(),
            Dimension::XYZ => !self.line_string_xyz.is_empty(),
        }
    }

    pub(crate) fn has_polygons(&self, dim: Dimension) -> bool {
        match dim {
            Dimension::XY => !self.polygon_xy.is_empty(),
            Dimension::XYZ => !self.polygon_xyz.is_empty(),
        }
    }

    pub(crate) fn has_multi_points(&self, dim: Dimension) -> bool {
        match dim {
            Dimension::XY => !self.mpoint_xy.is_empty(),
            Dimension::XYZ => !self.mpoint_xyz.is_empty(),
        }
    }

    pub(crate) fn has_multi_line_strings(&self, dim: Dimension) -> bool {
        match dim {
            Dimension::XY => !self.mline_string_xy.is_empty(),
            Dimension::XYZ => !self.mline_string_xyz.is_empty(),
        }
    }

    pub(crate) fn has_multi_polygons(&self, dim: Dimension) -> bool {
        match dim {
            Dimension::XY => !self.mpolygon_xy.is_empty(),
            Dimension::XYZ => !self.mpolygon_xyz.is_empty(),
        }
    }

    pub(crate) fn has_geometry_collections(&self, dim: Dimension) -> bool {
        match dim {
            Dimension::XY => !self.gc_xy.is_empty(),
            Dimension::XYZ => !self.gc_xyz.is_empty(),
        }
    }

    /// Return `true` if this array holds at least one geometry array of the given dimension
    pub fn has_dimension(&self, dim: Dimension) -> bool {
        use Dimension::*;
        match dim {
            XY => {
                self.has_points(XY)
                    || self.has_line_strings(XY)
                    || self.has_polygons(XY)
                    || self.has_multi_points(XY)
                    || self.has_multi_line_strings(XY)
                    || self.has_multi_polygons(XY)
            }
            XYZ => {
                self.has_points(XYZ)
                    || self.has_line_strings(XYZ)
                    || self.has_polygons(XYZ)
                    || self.has_multi_points(XYZ)
                    || self.has_multi_line_strings(XYZ)
                    || self.has_multi_polygons(XYZ)
            }
        }
    }

    /// Return `true` if this array holds at least one geometry array of the given dimension and no
    /// arrays of any other dimension.
    pub fn has_only_dimension(&self, dim: Dimension) -> bool {
        use Dimension::*;
        match dim {
            XY => self.has_dimension(XY) && !self.has_dimension(XYZ),
            XYZ => self.has_dimension(XYZ) && !self.has_dimension(XY),
        }
    }

    // Handle sliced data before downcasting.
    // pub fn compact_children()

    // /// The number of non-empty child arrays
    // fn num_non_empty_children(&self) -> usize {
    //     let mut count = 0;

    //     if !self.point_xy.is_empty() {
    //         count += 1
    //     };
    //     if !self.line_string_xy.is_empty() {
    //         count += 1
    //     };
    //     if !self.polygon_xy.is_empty() {
    //         count += 1
    //     };
    //     if !self.mpoint_xy.is_empty() {
    //         count += 1
    //     };
    //     if !self.mline_string_xy.is_empty() {
    //         count += 1
    //     };
    //     if !self.mpolygon_xy.is_empty() {
    //         count += 1
    //     };

    //     if !self.point_xyz.is_empty() {
    //         count += 1
    //     };
    //     if !self.line_string_xyz.is_empty() {
    //         count += 1
    //     };
    //     if !self.polygon_xyz.is_empty() {
    //         count += 1
    //     };
    //     if !self.mpoint_xyz.is_empty() {
    //         count += 1
    //     };
    //     if !self.mline_string_xyz.is_empty() {
    //         count += 1
    //     };
    //     if !self.mpolygon_xyz.is_empty() {
    //         count += 1
    //     };

    //     count
    // }

    // TODO: restore to enable downcasting

    // pub fn has_only_type(&self, typ: NativeType) -> bool {
    //     use Dimension::*;

    //     if self.num_non_empty_children() == 0 {
    //         // Empty array
    //         false
    //     }

    //     if self.num_non_empty_children() > 1 {}

    //     match typ {
    //         NativeType::Point(_, dim)
    //     }

    //     self.has_points(XY)
    //         && !self.has_line_strings(XY)
    //         && !self.has_polygons(XY)
    //         && !self.has_multi_points(XY)
    //         && !self.has_multi_line_strings(XY)
    //         && !self.has_multi_polygons(XY)
    //         && !self.has_points(XYZ)
    //         && !self.has_line_strings(XYZ)
    //         && !self.has_polygons(XYZ)
    //         && !self.has_multi_points(XYZ)
    //         && !self.has_multi_line_strings(XYZ)
    //         && !self.has_multi_polygons(XYZ)
    // }

    // pub fn has_only_line_strings(&self) -> bool {
    //     !self.has_points()
    //         && self.has_line_strings()
    //         && !self.has_polygons()
    //         && !self.has_multi_points()
    //         && !self.has_multi_line_strings()
    //         && !self.has_multi_polygons()
    // }

    // pub fn has_only_polygons(&self) -> bool {
    //     !self.has_points()
    //         && !self.has_line_strings()
    //         && self.has_polygons()
    //         && !self.has_multi_points()
    //         && !self.has_multi_line_strings()
    //         && !self.has_multi_polygons()
    // }

    // pub fn has_only_multi_points(&self) -> bool {
    //     !self.has_points()
    //         && !self.has_line_strings()
    //         && !self.has_polygons()
    //         && self.has_multi_points()
    //         && !self.has_multi_line_strings()
    //         && !self.has_multi_polygons()
    // }

    // pub fn has_only_multi_line_strings(&self) -> bool {
    //     !self.has_points()
    //         && !self.has_line_strings()
    //         && !self.has_polygons()
    //         && !self.has_multi_points()
    //         && self.has_multi_line_strings()
    //         && !self.has_multi_polygons()
    // }

    // pub fn has_only_multi_polygons(&self) -> bool {
    //     !self.has_points()
    //         && !self.has_line_strings()
    //         && !self.has_polygons()
    //         && !self.has_multi_points()
    //         && !self.has_multi_line_strings()
    //         && self.has_multi_polygons()
    // }

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

            point_xy: self.point_xy.clone(),
            line_string_xy: self.line_string_xy.clone(),
            polygon_xy: self.polygon_xy.clone(),
            mpoint_xy: self.mpoint_xy.clone(),
            mline_string_xy: self.mline_string_xy.clone(),
            mpolygon_xy: self.mpolygon_xy.clone(),
            gc_xy: self.gc_xy.clone(),

            point_xyz: self.point_xyz.clone(),
            line_string_xyz: self.line_string_xyz.clone(),
            polygon_xyz: self.polygon_xyz.clone(),
            mpoint_xyz: self.mpoint_xyz.clone(),
            mline_string_xyz: self.mline_string_xyz.clone(),
            mpolygon_xyz: self.mpolygon_xyz.clone(),
            gc_xyz: self.gc_xyz.clone(),

            metadata: self.metadata.clone(),
        }
    }

    /// Change the coordinate type of this array.
    pub fn to_coord_type(&self, coord_type: CoordType) -> Self {
        self.clone().into_coord_type(coord_type)
    }

    /// Change the coordinate type of this array.
    pub fn into_coord_type(self, coord_type: CoordType) -> Self {
        Self::new(
            self.type_ids,
            self.offsets,
            Some(self.point_xy.into_coord_type(coord_type)),
            Some(self.line_string_xy.into_coord_type(coord_type)),
            Some(self.polygon_xy.into_coord_type(coord_type)),
            Some(self.mpoint_xy.into_coord_type(coord_type)),
            Some(self.mline_string_xy.into_coord_type(coord_type)),
            Some(self.mpolygon_xy.into_coord_type(coord_type)),
            Some(self.gc_xy.into_coord_type(coord_type)),
            Some(self.point_xyz.into_coord_type(coord_type)),
            Some(self.line_string_xyz.into_coord_type(coord_type)),
            Some(self.polygon_xyz.into_coord_type(coord_type)),
            Some(self.mpoint_xyz.into_coord_type(coord_type)),
            Some(self.mline_string_xyz.into_coord_type(coord_type)),
            Some(self.mpolygon_xyz.into_coord_type(coord_type)),
            Some(self.gc_xyz.into_coord_type(coord_type)),
            self.metadata,
        )
    }

    // TODO: recursively expand the types from the geometry collection array
    #[allow(dead_code)]
    pub(crate) fn contained_types(&self) -> HashSet<NativeType> {
        let mut types = HashSet::new();
        if self.has_points(Dimension::XY) {
            types.insert(self.point_xy.data_type());
        }
        if self.has_line_strings(Dimension::XY) {
            types.insert(self.line_string_xy.data_type());
        }
        if self.has_polygons(Dimension::XY) {
            types.insert(self.polygon_xy.data_type());
        }
        if self.has_multi_points(Dimension::XY) {
            types.insert(self.mpoint_xy.data_type());
        }
        if self.has_multi_line_strings(Dimension::XY) {
            types.insert(self.mline_string_xy.data_type());
        }
        if self.has_multi_polygons(Dimension::XY) {
            types.insert(self.mpolygon_xy.data_type());
        }
        if self.has_geometry_collections(Dimension::XY) {
            types.insert(self.gc_xy.data_type());
        }

        if self.has_points(Dimension::XYZ) {
            types.insert(self.point_xyz.data_type());
        }
        if self.has_line_strings(Dimension::XYZ) {
            types.insert(self.line_string_xyz.data_type());
        }
        if self.has_polygons(Dimension::XYZ) {
            types.insert(self.polygon_xyz.data_type());
        }
        if self.has_multi_points(Dimension::XYZ) {
            types.insert(self.mpoint_xyz.data_type());
        }
        if self.has_multi_line_strings(Dimension::XYZ) {
            types.insert(self.mline_string_xyz.data_type());
        }
        if self.has_multi_polygons(Dimension::XYZ) {
            types.insert(self.mpolygon_xyz.data_type());
        }
        if self.has_geometry_collections(Dimension::XYZ) {
            types.insert(self.gc_xyz.data_type());
        }

        types
    }
}

impl ArrayBase for GeometryArray {
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

    fn into_array_ref(self) -> ArrayRef {
        Arc::new(self.into_arrow())
    }

    fn to_array_ref(&self) -> ArrayRef {
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

impl NativeArray for GeometryArray {
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

impl GeometryArraySelfMethods for GeometryArray {
    fn with_coords(self, _coords: crate::array::CoordBuffer) -> Self {
        todo!();
    }

    fn into_coord_type(self, _coord_type: crate::array::CoordType) -> Self {
        todo!();
    }
}

impl NativeGeometryAccessor for GeometryArray {
    unsafe fn value_as_geometry_unchecked(&self, index: usize) -> crate::scalar::Geometry {
        let type_id = self.type_ids[index];
        let offset = self.offsets[index] as usize;

        match type_id {
            1 => Geometry::Point(self.point_xy.value(offset)),
            2 => Geometry::LineString(self.line_string_xy.value(offset)),
            3 => Geometry::Polygon(self.polygon_xy.value(offset)),
            4 => Geometry::MultiPoint(self.mpoint_xy.value(offset)),
            5 => Geometry::MultiLineString(self.mline_string_xy.value(offset)),
            6 => Geometry::MultiPolygon(self.mpolygon_xy.value(offset)),
            7 => Geometry::GeometryCollection(self.gc_xy.value(offset)),
            11 => Geometry::Point(self.point_xyz.value(offset)),
            12 => Geometry::LineString(self.line_string_xyz.value(offset)),
            13 => Geometry::Polygon(self.polygon_xyz.value(offset)),
            14 => Geometry::MultiPoint(self.mpoint_xyz.value(offset)),
            15 => Geometry::MultiLineString(self.mline_string_xyz.value(offset)),
            16 => Geometry::MultiPolygon(self.mpolygon_xyz.value(offset)),
            17 => Geometry::GeometryCollection(self.gc_xyz.value(offset)),
            _ => panic!("unknown type_id {}", type_id),
        }
    }
}

#[cfg(feature = "geos")]
impl<'a> crate::trait_::NativeGEOSGeometryAccessor<'a> for GeometryArray {
    unsafe fn value_as_geometry_unchecked(
        &'a self,
        index: usize,
    ) -> std::result::Result<geos::Geometry, geos::Error> {
        let geom = NativeGeometryAccessor::value_as_geometry_unchecked(self, index);
        (&geom).try_into()
    }
}

impl<'a> ArrayAccessor<'a> for GeometryArray {
    type Item = Geometry<'a>;
    type ItemGeo = geo::Geometry;

    unsafe fn value_unchecked(&'a self, index: usize) -> Self::Item {
        let type_id = self.type_ids[index];
        let offset = self.offsets[index] as usize;

        match type_id {
            1 => Geometry::Point(self.point_xy.value(offset)),
            2 => Geometry::LineString(self.line_string_xy.value(offset)),
            3 => Geometry::Polygon(self.polygon_xy.value(offset)),
            4 => Geometry::MultiPoint(self.mpoint_xy.value(offset)),
            5 => Geometry::MultiLineString(self.mline_string_xy.value(offset)),
            6 => Geometry::MultiPolygon(self.mpolygon_xy.value(offset)),
            7 => Geometry::GeometryCollection(self.gc_xy.value(offset)),
            11 => Geometry::Point(self.point_xyz.value(offset)),
            12 => Geometry::LineString(self.line_string_xyz.value(offset)),
            13 => Geometry::Polygon(self.polygon_xyz.value(offset)),
            14 => Geometry::MultiPoint(self.mpoint_xyz.value(offset)),
            15 => Geometry::MultiLineString(self.mline_string_xyz.value(offset)),
            16 => Geometry::MultiPolygon(self.mpolygon_xyz.value(offset)),
            17 => Geometry::GeometryCollection(self.gc_xyz.value(offset)),
            _ => panic!("unknown type_id {}", type_id),
        }
    }
}

impl IntoArrow for GeometryArray {
    type ArrowArray = UnionArray;

    fn into_arrow(self) -> Self::ArrowArray {
        let union_fields = match self.data_type.to_data_type() {
            DataType::Union(union_fields, _) => union_fields,
            _ => unreachable!(),
        };

        let child_arrays = vec![
            self.point_xy.into_array_ref(),
            self.line_string_xy.into_array_ref(),
            self.polygon_xy.into_array_ref(),
            self.mpoint_xy.into_array_ref(),
            self.mline_string_xy.into_array_ref(),
            self.mpolygon_xy.into_array_ref(),
            self.gc_xy.into_array_ref(),
            self.point_xyz.into_array_ref(),
            self.line_string_xyz.into_array_ref(),
            self.polygon_xyz.into_array_ref(),
            self.mpoint_xyz.into_array_ref(),
            self.mline_string_xyz.into_array_ref(),
            self.mpolygon_xyz.into_array_ref(),
            self.gc_xyz.into_array_ref(),
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

impl TryFrom<&UnionArray> for GeometryArray {
    type Error = GeoArrowError;

    fn try_from(value: &UnionArray) -> std::result::Result<Self, Self::Error> {
        let mut point_xy: Option<PointArray> = None;
        let mut line_string_xy: Option<LineStringArray> = None;
        let mut polygon_xy: Option<PolygonArray> = None;
        let mut mpoint_xy: Option<MultiPointArray> = None;
        let mut mline_string_xy: Option<MultiLineStringArray> = None;
        let mut mpolygon_xy: Option<MultiPolygonArray> = None;
        let mut gc_xy: Option<GeometryCollectionArray> = None;

        let mut point_xyz: Option<PointArray> = None;
        let mut line_string_xyz: Option<LineStringArray> = None;
        let mut polygon_xyz: Option<PolygonArray> = None;
        let mut mpoint_xyz: Option<MultiPointArray> = None;
        let mut mline_string_xyz: Option<MultiLineStringArray> = None;
        let mut mpolygon_xyz: Option<MultiPolygonArray> = None;
        let mut gc_xyz: Option<GeometryCollectionArray> = None;

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
                        1 => {
                            point_xy = Some(
                                (value.child(type_id).as_ref(), dimension)
                                    .try_into()
                                    .unwrap(),
                            );
                        }
                        2 => {
                            line_string_xy = Some(
                                (value.child(type_id).as_ref(), dimension)
                                    .try_into()
                                    .unwrap(),
                            );
                        }
                        3 => {
                            polygon_xy = Some(
                                (value.child(type_id).as_ref(), dimension)
                                    .try_into()
                                    .unwrap(),
                            );
                        }
                        4 => {
                            mpoint_xy = Some(
                                (value.child(type_id).as_ref(), dimension)
                                    .try_into()
                                    .unwrap(),
                            );
                        }
                        5 => {
                            mline_string_xy = Some(
                                (value.child(type_id).as_ref(), dimension)
                                    .try_into()
                                    .unwrap(),
                            );
                        }
                        6 => {
                            mpolygon_xy = Some(
                                (value.child(type_id).as_ref(), dimension)
                                    .try_into()
                                    .unwrap(),
                            );
                        }
                        7 => {
                            gc_xy = Some(
                                (value.child(type_id).as_ref(), dimension)
                                    .try_into()
                                    .unwrap(),
                            );
                        }
                        11 => {
                            point_xyz = Some(
                                (value.child(type_id).as_ref(), dimension)
                                    .try_into()
                                    .unwrap(),
                            );
                        }
                        12 => {
                            line_string_xyz = Some(
                                (value.child(type_id).as_ref(), dimension)
                                    .try_into()
                                    .unwrap(),
                            );
                        }
                        13 => {
                            polygon_xyz = Some(
                                (value.child(type_id).as_ref(), dimension)
                                    .try_into()
                                    .unwrap(),
                            );
                        }
                        14 => {
                            mpoint_xyz = Some(
                                (value.child(type_id).as_ref(), dimension)
                                    .try_into()
                                    .unwrap(),
                            );
                        }
                        15 => {
                            mline_string_xyz = Some(
                                (value.child(type_id).as_ref(), dimension)
                                    .try_into()
                                    .unwrap(),
                            );
                        }
                        16 => {
                            mpolygon_xyz = Some(
                                (value.child(type_id).as_ref(), dimension)
                                    .try_into()
                                    .unwrap(),
                            );
                        }
                        17 => {
                            gc_xyz = Some(
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

        Ok(Self::new(
            type_ids,
            offsets,
            point_xy,
            line_string_xy,
            polygon_xy,
            mpoint_xy,
            mline_string_xy,
            mpolygon_xy,
            gc_xy,
            point_xyz,
            line_string_xyz,
            polygon_xyz,
            mpoint_xyz,
            mline_string_xyz,
            mpolygon_xyz,
            gc_xyz,
            Default::default(),
        ))
    }
}

impl TryFrom<&dyn Array> for GeometryArray {
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

impl TryFrom<(&dyn Array, &Field)> for GeometryArray {
    type Error = GeoArrowError;

    fn try_from((arr, field): (&dyn Array, &Field)) -> Result<Self> {
        let mut arr: Self = arr.try_into()?;
        arr.metadata = Arc::new(ArrayMetadata::try_from(field)?);
        Ok(arr)
    }
}

impl<G: GeometryTrait<T = f64>> TryFrom<&[G]> for GeometryArray {
    type Error = GeoArrowError;

    fn try_from(geoms: &[G]) -> Result<Self> {
        let mut_arr: GeometryBuilder = geoms.try_into()?;
        Ok(mut_arr.into())
    }
}

impl<G: GeometryTrait<T = f64>> TryFrom<Vec<Option<G>>> for GeometryArray {
    type Error = GeoArrowError;

    fn try_from(geoms: Vec<Option<G>>) -> Result<Self> {
        let mut_arr: GeometryBuilder = geoms.try_into()?;
        Ok(mut_arr.into())
    }
}

impl<O: OffsetSizeTrait> TryFrom<WKBArray<O>> for GeometryArray {
    type Error = GeoArrowError;

    fn try_from(value: WKBArray<O>) -> Result<Self> {
        let mut_arr: GeometryBuilder = value.try_into()?;
        Ok(mut_arr.into())
    }
}

impl From<PointArray> for GeometryArray {
    fn from(value: PointArray) -> Self {
        let dim = value.dimension();
        let type_ids = match dim {
            Dimension::XY => vec![1; value.len()],
            Dimension::XYZ => vec![11; value.len()],
        }
        .into();
        let offsets = ScalarBuffer::from_iter(0..value.len() as i32);
        let metadata = value.metadata().clone();
        match dim {
            Dimension::XY => Self::new(
                type_ids,
                offsets,
                Some(value),
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                metadata,
            ),
            Dimension::XYZ => Self::new(
                type_ids,
                offsets,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                Some(value),
                None,
                None,
                None,
                None,
                None,
                None,
                metadata,
            ),
        }
    }
}

impl From<LineStringArray> for GeometryArray {
    fn from(value: LineStringArray) -> Self {
        let dim = value.dimension();
        let type_ids = match dim {
            Dimension::XY => vec![2; value.len()],
            Dimension::XYZ => vec![12; value.len()],
        }
        .into();
        let offsets = ScalarBuffer::from_iter(0..value.len() as i32);
        let metadata = value.metadata().clone();
        match dim {
            Dimension::XY => Self::new(
                type_ids,
                offsets,
                None,
                Some(value),
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                metadata,
            ),
            Dimension::XYZ => Self::new(
                type_ids,
                offsets,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                Some(value),
                None,
                None,
                None,
                None,
                None,
                metadata,
            ),
        }
    }
}

impl From<PolygonArray> for GeometryArray {
    fn from(value: PolygonArray) -> Self {
        let dim = value.dimension();
        let type_ids = match dim {
            Dimension::XY => vec![3; value.len()],
            Dimension::XYZ => vec![13; value.len()],
        }
        .into();
        let offsets = ScalarBuffer::from_iter(0..value.len() as i32);
        let metadata = value.metadata().clone();
        match dim {
            Dimension::XY => Self::new(
                type_ids,
                offsets,
                None,
                None,
                Some(value),
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                metadata,
            ),
            Dimension::XYZ => Self::new(
                type_ids,
                offsets,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                Some(value),
                None,
                None,
                None,
                None,
                metadata,
            ),
        }
    }
}

impl From<MultiPointArray> for GeometryArray {
    fn from(value: MultiPointArray) -> Self {
        let dim = value.dimension();
        let type_ids = match dim {
            Dimension::XY => vec![4; value.len()],
            Dimension::XYZ => vec![14; value.len()],
        }
        .into();
        let offsets = ScalarBuffer::from_iter(0..value.len() as i32);
        let metadata = value.metadata().clone();
        match dim {
            Dimension::XY => Self::new(
                type_ids,
                offsets,
                None,
                None,
                None,
                Some(value),
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                metadata,
            ),
            Dimension::XYZ => Self::new(
                type_ids,
                offsets,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                Some(value),
                None,
                None,
                None,
                metadata,
            ),
        }
    }
}

impl From<MultiLineStringArray> for GeometryArray {
    fn from(value: MultiLineStringArray) -> Self {
        let dim = value.dimension();
        let type_ids = match dim {
            Dimension::XY => vec![5; value.len()],
            Dimension::XYZ => vec![15; value.len()],
        }
        .into();
        let offsets = ScalarBuffer::from_iter(0..value.len() as i32);
        let metadata = value.metadata().clone();
        match dim {
            Dimension::XY => Self::new(
                type_ids,
                offsets,
                None,
                None,
                None,
                None,
                Some(value),
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                metadata,
            ),
            Dimension::XYZ => Self::new(
                type_ids,
                offsets,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                Some(value),
                None,
                None,
                metadata,
            ),
        }
    }
}

impl From<MultiPolygonArray> for GeometryArray {
    fn from(value: MultiPolygonArray) -> Self {
        let dim = value.dimension();
        let type_ids = match dim {
            Dimension::XY => vec![6; value.len()],
            Dimension::XYZ => vec![16; value.len()],
        }
        .into();
        let offsets = ScalarBuffer::from_iter(0..value.len() as i32);
        let metadata = value.metadata().clone();
        match dim {
            Dimension::XY => Self::new(
                type_ids,
                offsets,
                None,
                None,
                None,
                None,
                None,
                Some(value),
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                metadata,
            ),
            Dimension::XYZ => Self::new(
                type_ids,
                offsets,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                Some(value),
                None,
                metadata,
            ),
        }
    }
}

impl From<GeometryCollectionArray> for GeometryArray {
    fn from(value: GeometryCollectionArray) -> Self {
        let dim = value.dimension();
        let type_ids = match dim {
            Dimension::XY => vec![7; value.len()],
            Dimension::XYZ => vec![17; value.len()],
        }
        .into();
        let offsets = ScalarBuffer::from_iter(0..value.len() as i32);
        let metadata = value.metadata().clone();
        match dim {
            Dimension::XY => Self::new(
                type_ids,
                offsets,
                None,
                None,
                None,
                None,
                None,
                None,
                Some(value),
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                metadata,
            ),
            Dimension::XYZ => Self::new(
                type_ids,
                offsets,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                Some(value),
                metadata,
            ),
        }
    }
}

impl From<MixedGeometryArray> for GeometryArray {
    fn from(value: MixedGeometryArray) -> Self {
        use Dimension::*;

        // TODO: include slice offset
        let mut point_xy: Option<PointArray> = None;
        let mut line_string_xy: Option<LineStringArray> = None;
        let mut polygon_xy: Option<PolygonArray> = None;
        let mut mpoint_xy: Option<MultiPointArray> = None;
        let mut mline_string_xy: Option<MultiLineStringArray> = None;
        let mut mpolygon_xy: Option<MultiPolygonArray> = None;

        let mut point_xyz: Option<PointArray> = None;
        let mut line_string_xyz: Option<LineStringArray> = None;
        let mut polygon_xyz: Option<PolygonArray> = None;
        let mut mpoint_xyz: Option<MultiPointArray> = None;
        let mut mline_string_xyz: Option<MultiLineStringArray> = None;
        let mut mpolygon_xyz: Option<MultiPolygonArray> = None;

        match value.dimension() {
            XY => {
                point_xy = Some(value.points);
                line_string_xy = Some(value.line_strings);
                polygon_xy = Some(value.polygons);
                mpoint_xy = Some(value.multi_points);
                mline_string_xy = Some(value.multi_line_strings);
                mpolygon_xy = Some(value.multi_polygons);
            }
            XYZ => {
                point_xyz = Some(value.points);
                line_string_xyz = Some(value.line_strings);
                polygon_xyz = Some(value.polygons);
                mpoint_xyz = Some(value.multi_points);
                mline_string_xyz = Some(value.multi_line_strings);
                mpolygon_xyz = Some(value.multi_polygons);
            }
        }

        Self::new(
            value.type_ids,
            value.offsets,
            point_xy,
            line_string_xy,
            polygon_xy,
            mpoint_xy,
            mline_string_xy,
            mpolygon_xy,
            None,
            point_xyz,
            line_string_xyz,
            polygon_xyz,
            mpoint_xyz,
            mline_string_xyz,
            mpolygon_xyz,
            None,
            value.metadata,
        )
    }
}

impl TryFrom<GeometryArray> for MixedGeometryArray {
    type Error = GeoArrowError;

    /// Will error if:
    ///
    /// - the contained geometries are not all of the same dimension
    /// - any geometry collection child exists
    fn try_from(value: GeometryArray) -> std::result::Result<Self, Self::Error> {
        if value.has_only_dimension(Dimension::XY) {
            if value.gc_xy.is_empty() {
                Ok(MixedGeometryArray::new(
                    value.type_ids,
                    value.offsets,
                    Some(value.point_xy),
                    Some(value.line_string_xy),
                    Some(value.polygon_xy),
                    Some(value.mpoint_xy),
                    Some(value.mline_string_xy),
                    Some(value.mpolygon_xy),
                    value.metadata,
                ))
            } else {
                Err(GeoArrowError::General(
                    "Cannot cast to MixedGeometryArray with non-empty GeometryCollection child."
                        .to_string(),
                ))
            }
        } else if value.has_only_dimension(Dimension::XYZ) {
            if value.gc_xyz.is_empty() {
                Ok(MixedGeometryArray::new(
                    value.type_ids,
                    value.offsets,
                    Some(value.point_xyz),
                    Some(value.line_string_xyz),
                    Some(value.polygon_xyz),
                    Some(value.mpoint_xyz),
                    Some(value.mline_string_xyz),
                    Some(value.mpolygon_xyz),
                    value.metadata,
                ))
            } else {
                Err(GeoArrowError::General(
                    "Cannot cast to MixedGeometryArray with non-empty GeometryCollection child."
                        .to_string(),
                ))
            }
        } else {
            Err(GeoArrowError::General(
                "Cannot cast to MixedGeometryArray when GeometryArray contains multiple dimensions"
                    .to_string(),
            ))
        }
    }
}

impl From<RectArray> for GeometryArray {
    fn from(value: RectArray) -> Self {
        PolygonArray::from(value).into()
    }
}

impl From<Arc<dyn NativeArray>> for GeometryArray {
    fn from(value: Arc<dyn NativeArray>) -> Self {
        use NativeType::*;

        match value.data_type() {
            Point(_, _) => value.as_ref().as_point().clone().into(),
            LineString(_, _) => value.as_ref().as_line_string().clone().into(),
            Polygon(_, _) => value.as_ref().as_polygon().clone().into(),
            MultiPoint(_, _) => value.as_ref().as_multi_point().clone().into(),
            MultiLineString(_, _) => value.as_ref().as_multi_line_string().clone().into(),
            MultiPolygon(_, _) => value.as_ref().as_multi_polygon().clone().into(),
            Geometry(_) => value.as_ref().as_geometry().clone(),
            GeometryCollection(_, _) => value.as_ref().as_geometry_collection().clone().into(),
            Rect(_) => value.as_ref().as_rect().clone().into(),
        }
    }
}

/// Default to an empty array
impl Default for GeometryArray {
    fn default() -> Self {
        GeometryBuilder::default().into()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test::{linestring, multilinestring, multipoint, multipolygon, point, polygon};

    #[test]
    fn geo_roundtrip_accurate_points() {
        let geoms: Vec<geo::Geometry> = vec![
            geo::Geometry::Point(point::p0()),
            geo::Geometry::Point(point::p1()),
            geo::Geometry::Point(point::p2()),
        ];

        let arr: GeometryArray = GeometryBuilder::from_geometries(
            geoms.as_slice(),
            Default::default(),
            Default::default(),
            false,
        )
        .unwrap()
        .finish();

        assert_eq!(arr.value_as_geo(0), geo::Geometry::Point(point::p0()));
        assert_eq!(arr.value_as_geo(1), geo::Geometry::Point(point::p1()));
        assert_eq!(arr.value_as_geo(2), geo::Geometry::Point(point::p2()));
    }

    #[test]
    fn geo_roundtrip_accurate_multi_points() {
        let geoms: Vec<geo::Geometry> = vec![
            geo::Geometry::Point(point::p0()),
            geo::Geometry::Point(point::p1()),
            geo::Geometry::Point(point::p2()),
        ];
        let arr: GeometryArray = GeometryBuilder::from_geometries(
            geoms.as_slice(),
            Default::default(),
            Default::default(),
            true,
        )
        .unwrap()
        .finish();

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

        let arr: GeometryArray = GeometryBuilder::from_geometries(
            geoms.as_slice(),
            Default::default(),
            Default::default(),
            false,
        )
        .unwrap()
        .finish();

        assert_eq!(arr.value_as_geo(0), geoms[0]);
        assert_eq!(arr.value_as_geo(1), geoms[1]);
        assert_eq!(arr.value_as_geo(2), geoms[2]);
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

        let arr: GeometryArray = GeometryBuilder::from_geometries(
            geoms.as_slice(),
            Default::default(),
            Default::default(),
            false,
        )
        .unwrap()
        .finish();

        // Round trip to/from arrow-rs
        let arrow_array = arr.into_arrow();
        let round_trip_arr: GeometryArray = (&arrow_array).try_into().unwrap();

        assert_eq!(round_trip_arr.value_as_geo(0), geoms[0]);
        assert_eq!(round_trip_arr.value_as_geo(1), geoms[1]);
        assert_eq!(round_trip_arr.value_as_geo(2), geoms[2]);
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

        let arr: GeometryArray = GeometryBuilder::from_geometries(
            geoms.as_slice(),
            Default::default(),
            Default::default(),
            false,
        )
        .unwrap()
        .finish();

        // Round trip to/from arrow-rs
        let arrow_array = arr.into_arrow();
        let round_trip_arr: GeometryArray = (&arrow_array).try_into().unwrap();

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

        let arr: GeometryArray = GeometryBuilder::from_geometries(
            geoms.as_slice(),
            Default::default(),
            Default::default(),
            false,
        )
        .unwrap()
        .finish();

        // Round trip to/from arrow-rs
        let arrow_array = arr.into_arrow();
        let round_trip_arr: GeometryArray = (&arrow_array).try_into().unwrap();

        assert_eq!(round_trip_arr.value_as_geo(0), geoms[0]);
        assert_eq!(round_trip_arr.value_as_geo(1), geoms[1]);
    }

    #[test]
    fn test_slicing() {
        let geoms: Vec<geo::Geometry> = vec![
            geo::Geometry::Point(point::p0()),
            geo::Geometry::LineString(linestring::ls0()),
            geo::Geometry::Polygon(polygon::p0()),
            geo::Geometry::MultiPoint(multipoint::mp0()),
            geo::Geometry::MultiLineString(multilinestring::ml0()),
            geo::Geometry::MultiPolygon(multipolygon::mp0()),
        ];

        let arr: GeometryArray = GeometryBuilder::from_geometries(
            geoms.as_slice(),
            Default::default(),
            Default::default(),
            false,
        )
        .unwrap()
        .finish();

        assert_eq!(arr.slice(1, 2).value_as_geo(0), geoms[1]);
        assert_eq!(arr.slice(1, 2).value_as_geo(1), geoms[2]);
        assert_eq!(arr.slice(3, 3).value_as_geo(2), geoms[5]);
    }
}
