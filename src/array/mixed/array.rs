use arrow2::array::UnionArray;
use arrow2::bitmap::Bitmap;
use arrow2::buffer::Buffer;
use arrow2::datatypes::DataType;
use arrow2::types::Offset;
use rstar::primitives::CachedEnvelope;
use rstar::RTree;

use crate::array::{
    LineStringArray, MultiLineStringArray, MultiPointArray, MultiPolygonArray, PointArray,
    PolygonArray,
};
use crate::scalar::Geometry;
use crate::GeometryArrayTrait;

/// # Invariants
///
/// - All arrays must have the same dimension
/// - All arrays must have the same coordinate layout (interleaved or separated)
#[derive(Debug, Clone, PartialEq)]
pub struct MixedGeometryArray<O: Offset> {
    // Invariant: every item in `types` is `> 0 && < fields.len()`
    // - 0: PointArray
    // - 1: LineStringArray
    // - 2: PolygonArray
    // - 3: MultiPointArray
    // - 4: MultiLineStringArray
    // - 5: MultiPolygonArray
    types: Buffer<i8>,

    /// Note that we include an ordering so that exporting this array to Arrow is O(1). If we used
    /// another ordering like always Point, LineString, etc. then we'd either have to always export
    /// all arrays (including some zero-length arrays) or have to reorder the `types` buffer when
    /// exporting.
    // ordering: Vec<>,
    points: PointArray,
    line_strings: LineStringArray<O>,
    polygons: PolygonArray<O>,
    multi_points: MultiPointArray<O>,
    multi_line_strings: MultiLineStringArray<O>,
    multi_polygons: MultiPolygonArray<O>,

    // Invariant: `offsets.len() == types.len()`
    offsets: Buffer<i32>,
}

enum MixedGeometryOrdering {
    Point = 0,
    LineString = 1,
    Polygon = 2,
    MultiPoint = 3,
    MultiLineString = 4,
    MultiPolygon = 5,
}

impl From<i8> for MixedGeometryOrdering {
    fn from(value: i8) -> Self {
        match value {
            0 => MixedGeometryOrdering::Point,
            1 => MixedGeometryOrdering::LineString,
            2 => MixedGeometryOrdering::Polygon,
            3 => MixedGeometryOrdering::MultiPoint,
            4 => MixedGeometryOrdering::MultiLineString,
            5 => MixedGeometryOrdering::MultiPolygon,
            _ => panic!(),
        }
    }
}

impl<'a, O: Offset> GeometryArrayTrait<'a> for MixedGeometryArray<O> {
    type Scalar = Geometry<'a, O>;
    type ScalarGeo = geo::Geometry;
    type ArrowArray = UnionArray;
    type RTreeObject = CachedEnvelope<Self::Scalar>;

    /// Gets the value at slot `i`
    fn value(&'a self, i: usize) -> Self::Scalar {
        let index = self.types[i];
        let geometry_type = MixedGeometryOrdering::from(index);
        let offset = self.offsets[index as usize] as usize;
        match geometry_type {
            MixedGeometryOrdering::Point => Geometry::Point(self.points.value(offset)),
            MixedGeometryOrdering::LineString => {
                Geometry::LineString(self.line_strings.value(offset))
            }
            MixedGeometryOrdering::Polygon => Geometry::Polygon(self.polygons.value(offset)),
            MixedGeometryOrdering::MultiPoint => {
                Geometry::MultiPoint(self.multi_points.value(offset))
            }
            MixedGeometryOrdering::MultiLineString => {
                Geometry::MultiLineString(self.multi_line_strings.value(offset))
            }
            MixedGeometryOrdering::MultiPolygon => {
                Geometry::MultiPolygon(self.multi_polygons.value(offset))
            }
        }
    }

    fn logical_type(&self) -> DataType {
        todo!();
    }

    fn extension_type(&self) -> DataType {
        DataType::Extension(
            "geoarrow.mixed".to_string(),
            Box::new(self.logical_type()),
            None,
        )
    }

    fn into_arrow(self) -> Self::ArrowArray {
        let extension_type = self.extension_type();
        let fields = vec![
            self.points.into_boxed_arrow(),
            self.line_strings.into_boxed_arrow(),
            self.polygons.into_boxed_arrow(),
            self.multi_points.into_boxed_arrow(),
            self.multi_line_strings.into_boxed_arrow(),
            self.multi_polygons.into_boxed_arrow(),
        ];

        UnionArray::new(extension_type, self.types, fields, Some(self.offsets))
    }

    fn into_boxed_arrow(self) -> Box<dyn arrow2::array::Array> {
        self.into_arrow().boxed()
    }

    fn with_coords(self, coords: crate::array::CoordBuffer) -> Self {
        todo!();
    }

    fn coord_type(&self) -> crate::array::CoordType {
        todo!();
    }

    fn into_coord_type(self, coord_type: crate::array::CoordType) -> Self {
        todo!();
    }

    /// Build a spatial index containing this array's geometries
    fn rstar_tree(&'a self) -> RTree<Self::RTreeObject> {
        RTree::bulk_load(self.iter().flatten().map(CachedEnvelope::new).collect())
    }

    /// Returns the number of geometries in this array
    #[inline]
    fn len(&self) -> usize {
        self.types.len()
    }

    /// Returns the optional validity.
    #[inline]
    fn validity(&self) -> Option<&Bitmap> {
        None
    }

    /// Slices this [`MixedGeometryArray`] in place.
    ///
    /// # Implementation
    /// This operation is `O(1)` as it amounts to increase two ref counts.
    /// # Examples
    /// ```
    /// use arrow2::array::PrimitiveArray;
    ///
    /// let array = PrimitiveArray::from_vec(vec![1, 2, 3]);
    /// assert_eq!(format!("{:?}", array), "Int32[1, 2, 3]");
    /// let sliced = array.slice(1, 1);
    /// assert_eq!(format!("{:?}", sliced), "Int32[2]");
    /// // note: `sliced` and `array` share the same memory region.
    /// ```
    /// # Panic
    /// This function panics iff `offset + length > self.len()`.
    #[inline]
    fn slice(&mut self, offset: usize, length: usize) {
        assert!(
            offset + length <= self.len(),
            "offset + length may not exceed length of array"
        );
        unsafe { self.slice_unchecked(offset, length) }
    }

    /// Slices this [`MixedGeometryArray`] in place.
    ///
    /// # Implementation
    /// This operation is `O(1)` as it amounts to increase two ref counts.
    /// # Safety
    /// The caller must ensure that `offset + length <= self.len()`.
    #[inline]
    unsafe fn slice_unchecked(&mut self, offset: usize, length: usize) {
        todo!()
    }

    fn to_boxed(&self) -> Box<Self> {
        Box::new(self.clone())
    }
}

// Implement geometry accessors
impl<O: Offset> MixedGeometryArray<O> {
    /// Iterator over geo Geometry objects, not looking at validity
    pub fn iter_geo_values(&self) -> impl Iterator<Item = geo::Geometry> + '_ {
        (0..self.len()).map(|i| self.value_as_geo(i))
    }

    /// Iterator over geo Geometry objects, taking into account validity
    pub fn iter_geo(&self) -> impl Iterator<Item = Option<geo::Geometry>> + '_ {
        (0..self.len()).map(|i| self.get_as_geo(i))
    }

    /// Returns the value at slot `i` as a GEOS geometry.
    #[cfg(feature = "geos")]
    pub fn value_as_geos(&self, i: usize) -> geos::Geometry {
        self.value(i).try_into().unwrap()
    }

    /// Gets the value at slot `i` as a GEOS geometry, additionally checking the validity bitmap
    #[cfg(feature = "geos")]
    pub fn get_as_geos(&self, i: usize) -> Option<geos::Geometry> {
        self.get(i).map(|geom| geom.try_into().unwrap())
    }

    /// Iterator over GEOS geometry objects
    #[cfg(feature = "geos")]
    pub fn iter_geos_values(&self) -> impl Iterator<Item = geos::Geometry> + '_ {
        (0..self.len()).map(|i| self.value_as_geos(i))
    }

    /// Iterator over GEOS geometry objects, taking validity into account
    #[cfg(feature = "geos")]
    pub fn iter_geos(&self) -> impl Iterator<Item = Option<geos::Geometry>> + '_ {
        (0..self.len()).map(|i| self.get_as_geos(i))
    }
}
