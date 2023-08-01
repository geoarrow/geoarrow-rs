use arrow2::array::UnionArray;
use arrow2::bitmap::Bitmap;
use arrow2::buffer::Buffer;
use arrow2::datatypes::{DataType, Field, UnionMode};
use arrow2::types::Offset;
use rstar::primitives::CachedEnvelope;
use rstar::RTree;

use crate::array::mixed::mutable::MutableMixedGeometryArray;
use crate::array::{
    LineStringArray, MultiLineStringArray, MultiPointArray, MultiPolygonArray, PointArray,
    PolygonArray,
};
use crate::error::GeoArrowError;
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

    // Invariant: `offsets.len() == types.len()`
    offsets: Buffer<i32>,

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

    /// An offset used for slicing into this array. The offset will be 0 if the array has not been
    /// sliced.
    ///
    /// In order to slice this array efficiently (and zero-cost) we can't slice the underlying
    /// fields directly. If this were always a _sparse_ union array, we could! We could then always
    /// slice from offset to length of each underlying array. But we're under the assumption that
    /// most or all of the time we have a dense union array, where the `offsets` buffer is defined.
    /// In that case, to know how to slice each underlying array, we'd have to walk the `types` and
    /// `offsets` arrays (in O(N) time) to figure out how to slice the underlying arrays.
    ///
    /// Instead, we store the slice offset.
    ///
    /// Note that this offset is only for slicing into the **fields**, i.e. the geometry arrays.
    /// The `types` and `offsets` arrays are sliced as usual.
    ///
    /// TODO: when exporting this array, export to arrow2 and then slice from scratch because we
    /// can't set the `offset` in a UnionArray constructor
    slice_offset: usize,
}

#[derive(Debug, Clone, Copy)]
pub enum MixedGeometryOrdering {
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

impl From<MixedGeometryOrdering> for i8 {
    fn from(value: MixedGeometryOrdering) -> Self {
        match value {
            MixedGeometryOrdering::Point => 0,
            MixedGeometryOrdering::LineString => 1,
            MixedGeometryOrdering::Polygon => 2,
            MixedGeometryOrdering::MultiPoint => 3,
            MixedGeometryOrdering::MultiLineString => 4,
            MixedGeometryOrdering::MultiPolygon => 5,
        }
    }
}

impl<O: Offset> MixedGeometryArray<O> {
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
        types: Buffer<i8>,
        offsets: Buffer<i32>,
        points: PointArray,
        line_strings: LineStringArray<O>,
        polygons: PolygonArray<O>,
        multi_points: MultiPointArray<O>,
        multi_line_strings: MultiLineStringArray<O>,
        multi_polygons: MultiPolygonArray<O>,
    ) -> Self {
        Self {
            types,
            offsets,
            points,
            line_strings,
            polygons,
            multi_points,
            multi_line_strings,
            multi_polygons,
            slice_offset: 0,
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
        let offset = self.offsets[i] as usize;
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
        let mut fields: Vec<Field> = vec![];
        let mut ids = vec![];

        if self.points.len() > 0 {
            fields.push(Field::new("", self.points.extension_type(), true));
            ids.push(0);
        }
        if self.line_strings.len() > 0 {
            fields.push(Field::new("", self.line_strings.extension_type(), true));
            ids.push(1);
        }
        if self.polygons.len() > 0 {
            fields.push(Field::new("", self.polygons.extension_type(), true));
            ids.push(2);
        }
        if self.multi_points.len() > 0 {
            fields.push(Field::new("", self.multi_points.extension_type(), true));
            ids.push(3);
        }
        if self.multi_line_strings.len() > 0 {
            fields.push(Field::new(
                "",
                self.multi_line_strings.extension_type(),
                true,
            ));
            ids.push(4);
        }
        if self.multi_polygons.len() > 0 {
            fields.push(Field::new("", self.multi_polygons.extension_type(), true));
            ids.push(5);
        }

        DataType::Union(fields, Some(ids), UnionMode::Dense)
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
        let mut fields = vec![];

        if self.points.len() > 0 {
            fields.push(self.points.into_boxed_arrow());
        }
        if self.line_strings.len() > 0 {
            fields.push(self.line_strings.into_boxed_arrow());
        }
        if self.polygons.len() > 0 {
            fields.push(self.polygons.into_boxed_arrow());
        }
        if self.multi_points.len() > 0 {
            fields.push(self.multi_points.into_boxed_arrow());
        }
        if self.multi_line_strings.len() > 0 {
            fields.push(self.multi_line_strings.into_boxed_arrow());
        }
        if self.multi_polygons.len() > 0 {
            fields.push(self.multi_polygons.into_boxed_arrow());
        }

        UnionArray::new(extension_type, self.types, fields, Some(self.offsets))
    }

    fn into_boxed_arrow(self) -> Box<dyn arrow2::array::Array> {
        self.into_arrow().boxed()
    }

    fn with_coords(self, _coords: crate::array::CoordBuffer) -> Self {
        todo!();
    }

    fn coord_type(&self) -> crate::array::CoordType {
        todo!();
    }

    fn into_coord_type(self, _coord_type: crate::array::CoordType) -> Self {
        todo!();
    }

    /// Build a spatial index containing this array's geometries
    fn rstar_tree(&'a self) -> RTree<Self::RTreeObject> {
        RTree::bulk_load(self.iter().flatten().map(CachedEnvelope::new).collect())
    }

    /// Returns the number of geometries in this array
    #[inline]
    fn len(&self) -> usize {
        // Note that `types` is sliced as usual, and thus always has the correct length.
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
    ///
    /// This operation is `O(F)` where `F` is the number of fields.
    ///
    /// # Panic
    ///
    /// This function panics iff `offset + length >= self.len()`.
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
    ///
    /// This operation is `O(F)` where `F` is the number of fields.
    ///
    /// # Safety
    ///
    /// The caller must ensure that `offset + length <= self.len()`.
    #[inline]
    unsafe fn slice_unchecked(&mut self, offset: usize, length: usize) {
        debug_assert!(offset + length <= self.len());

        self.types.slice_unchecked(offset, length);
        self.offsets.slice_unchecked(offset, length);
        self.slice_offset += offset;
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

impl<O: Offset> TryFrom<Vec<geo::Geometry>> for MixedGeometryArray<O> {
    type Error = GeoArrowError;

    fn try_from(value: Vec<geo::Geometry>) -> Result<Self, Self::Error> {
        let mut_arr: MutableMixedGeometryArray<O> = value.try_into()?;
        Ok(mut_arr.into())
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
        let arr: MixedGeometryArray<i32> = geoms.try_into().unwrap();

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
        let arr: MixedGeometryArray<i32> = geoms.clone().try_into().unwrap();

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
        assert_eq!(arr.value_as_geo(3), geoms[3],);
        assert_eq!(arr.value_as_geo(4), geoms[4],);
        assert_eq!(arr.value_as_geo(5), geoms[5],);
    }
}
