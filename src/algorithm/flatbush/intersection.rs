use crate::algorithm::flatbush::index::FlatbushRTree;
use crate::array::*;
use crate::datatypes::GeoDataType;
use crate::trait_::GeometryArrayAccessor;
use crate::GeometryArrayTrait;
use arrow_array::builder::UInt32Builder;
use arrow_array::{OffsetSizeTrait, UInt32Array};
use flatbush::FlatbushIndex;
use geo::Intersects as _Intersects;

pub trait Intersects<Rhs = Self> {
    type Output;

    /// Find the indices of left and right where their geometries intersect.
    fn intersects_indices(&self, rhs: &Rhs) -> Self::Output;
}

impl Intersects for PointArray {
    type Output = (UInt32Array, UInt32Array);

    fn intersects_indices(&self, rhs: &Self) -> Self::Output {
        let left_tree = self.flatbush();
        let right_tree = rhs.flatbush();
        let indices = left_tree
            .intersection_candidates_with_other_tree(&right_tree)
            .collect::<Vec<_>>();

        // Unclear if we should use
        let mut left = UInt32Builder::with_capacity(indices.len());
        let mut right = UInt32Builder::with_capacity(indices.len());

        indices.into_iter().for_each(|(l, r)| {
            // Actually check intersection
            if self.value_as_geo(l).intersects(&rhs.value_as_geo(r)) {
                left.append_value(l.try_into().unwrap());
                right.append_value(r.try_into().unwrap());
            }
        });

        (left.finish(), right.finish())
    }
}

macro_rules! impl_array_single_offset {
    ($first:ty, $second:ty) => {
        impl<O: OffsetSizeTrait> Intersects<$second> for $first {
            type Output = (UInt32Array, UInt32Array);

            fn intersects_indices(&self, rhs: &$second) -> Self::Output {
                let left_tree = self.flatbush();
                let right_tree = rhs.flatbush();
                let indices = left_tree
                    .intersection_candidates_with_other_tree(&right_tree)
                    .collect::<Vec<_>>();

                // Unclear if we should use
                let mut left = UInt32Builder::with_capacity(indices.len());
                let mut right = UInt32Builder::with_capacity(indices.len());

                indices.into_iter().for_each(|(l, r)| {
                    // Actually check intersection
                    if self.value_as_geo(l).intersects(&rhs.value_as_geo(r)) {
                        left.append_value(l.try_into().unwrap());
                        right.append_value(r.try_into().unwrap());
                    }
                });

                (left.finish(), right.finish())
            }
        }
    };
}

// Implementations on PointArray
impl_array_single_offset!(PointArray, LineStringArray<O>);
impl_array_single_offset!(PointArray, PolygonArray<O>);
impl_array_single_offset!(PointArray, MultiPointArray<O>);
impl_array_single_offset!(PointArray, MultiLineStringArray<O>);
impl_array_single_offset!(PointArray, MultiPolygonArray<O>);
impl_array_single_offset!(PointArray, MixedGeometryArray<O>);
impl_array_single_offset!(PointArray, GeometryCollectionArray<O>);

// Reverse implementations
impl_array_single_offset!(LineStringArray<O>, PointArray);
impl_array_single_offset!(PolygonArray<O>, PointArray);
impl_array_single_offset!(MultiPointArray<O>, PointArray);
impl_array_single_offset!(MultiLineStringArray<O>, PointArray);
impl_array_single_offset!(MultiPolygonArray<O>, PointArray);
impl_array_single_offset!(MixedGeometryArray<O>, PointArray);
impl_array_single_offset!(GeometryCollectionArray<O>, PointArray);

macro_rules! impl_array_two_offsets {
    ($first:ty, $second:ty) => {
        impl<O1: OffsetSizeTrait, O2: OffsetSizeTrait> Intersects<$second> for $first {
            type Output = (UInt32Array, UInt32Array);

            fn intersects_indices(&self, rhs: &$second) -> Self::Output {
                let left_tree = self.flatbush();
                let right_tree = rhs.flatbush();
                let indices = left_tree
                    .intersection_candidates_with_other_tree(&right_tree)
                    .collect::<Vec<_>>();

                // Unclear if we should use
                let mut left = UInt32Builder::with_capacity(indices.len());
                let mut right = UInt32Builder::with_capacity(indices.len());

                indices.into_iter().for_each(|(l, r)| {
                    // Actually check intersection
                    if self.value_as_geo(l).intersects(&rhs.value_as_geo(r)) {
                        left.append_value(l.try_into().unwrap());
                        right.append_value(r.try_into().unwrap());
                    }
                });

                (left.finish(), right.finish())
            }
        }
    };
}

// Implementations on LineStringArray
impl_array_two_offsets!(LineStringArray<O1>, LineStringArray<O2>);
impl_array_two_offsets!(LineStringArray<O1>, PolygonArray<O2>);
impl_array_two_offsets!(LineStringArray<O1>, MultiPointArray<O2>);
impl_array_two_offsets!(LineStringArray<O1>, MultiLineStringArray<O2>);
impl_array_two_offsets!(LineStringArray<O1>, MultiPolygonArray<O2>);
impl_array_two_offsets!(LineStringArray<O1>, MixedGeometryArray<O2>);
impl_array_two_offsets!(LineStringArray<O1>, GeometryCollectionArray<O2>);

impl_array_two_offsets!(PolygonArray<O1>, LineStringArray<O2>);
impl_array_two_offsets!(MultiPointArray<O1>, LineStringArray<O2>);
impl_array_two_offsets!(MultiLineStringArray<O1>, LineStringArray<O2>);
impl_array_two_offsets!(MultiPolygonArray<O1>, LineStringArray<O2>);
impl_array_two_offsets!(MixedGeometryArray<O1>, LineStringArray<O2>);
impl_array_two_offsets!(GeometryCollectionArray<O1>, LineStringArray<O2>);

// Implementations on PolygonArray
impl_array_two_offsets!(PolygonArray<O1>, PolygonArray<O2>);
impl_array_two_offsets!(PolygonArray<O1>, MultiPointArray<O2>);
impl_array_two_offsets!(PolygonArray<O1>, MultiLineStringArray<O2>);
impl_array_two_offsets!(PolygonArray<O1>, MultiPolygonArray<O2>);
impl_array_two_offsets!(PolygonArray<O1>, MixedGeometryArray<O2>);
impl_array_two_offsets!(PolygonArray<O1>, GeometryCollectionArray<O2>);

impl_array_two_offsets!(MultiPointArray<O1>, PolygonArray<O2>);
impl_array_two_offsets!(MultiLineStringArray<O1>, PolygonArray<O2>);
impl_array_two_offsets!(MultiPolygonArray<O1>, PolygonArray<O2>);
impl_array_two_offsets!(MixedGeometryArray<O1>, PolygonArray<O2>);
impl_array_two_offsets!(GeometryCollectionArray<O1>, PolygonArray<O2>);

// Implementations on MultiPointArray
impl_array_two_offsets!(MultiPointArray<O1>, MultiPointArray<O2>);
impl_array_two_offsets!(MultiPointArray<O1>, MultiLineStringArray<O2>);
impl_array_two_offsets!(MultiPointArray<O1>, MultiPolygonArray<O2>);
impl_array_two_offsets!(MultiPointArray<O1>, MixedGeometryArray<O2>);
impl_array_two_offsets!(MultiPointArray<O1>, GeometryCollectionArray<O2>);

impl_array_two_offsets!(MultiLineStringArray<O1>, MultiPointArray<O2>);
impl_array_two_offsets!(MultiPolygonArray<O1>, MultiPointArray<O2>);
impl_array_two_offsets!(MixedGeometryArray<O1>, MultiPointArray<O2>);
impl_array_two_offsets!(GeometryCollectionArray<O1>, MultiPointArray<O2>);

// Implementations on MultiLineStringArray
impl_array_two_offsets!(MultiLineStringArray<O1>, MultiLineStringArray<O2>);
impl_array_two_offsets!(MultiLineStringArray<O1>, MultiPolygonArray<O2>);
impl_array_two_offsets!(MultiLineStringArray<O1>, MixedGeometryArray<O2>);
impl_array_two_offsets!(MultiLineStringArray<O1>, GeometryCollectionArray<O2>);

impl_array_two_offsets!(MultiPolygonArray<O1>, MultiLineStringArray<O2>);
impl_array_two_offsets!(MixedGeometryArray<O1>, MultiLineStringArray<O2>);
impl_array_two_offsets!(GeometryCollectionArray<O1>, MultiLineStringArray<O2>);

// Implementations on MultiPolygonArray
impl_array_two_offsets!(MultiPolygonArray<O1>, MultiPolygonArray<O2>);
impl_array_two_offsets!(MultiPolygonArray<O1>, MixedGeometryArray<O2>);
impl_array_two_offsets!(MultiPolygonArray<O1>, GeometryCollectionArray<O2>);

impl_array_two_offsets!(MixedGeometryArray<O1>, MultiPolygonArray<O2>);
impl_array_two_offsets!(GeometryCollectionArray<O1>, MultiPolygonArray<O2>);

// Implementations on MixedGeometryArray
impl_array_two_offsets!(MixedGeometryArray<O1>, MixedGeometryArray<O2>);
impl_array_two_offsets!(MixedGeometryArray<O1>, GeometryCollectionArray<O2>);

impl_array_two_offsets!(GeometryCollectionArray<O1>, MixedGeometryArray<O2>);

// Implementations on GeometryCollectionArray
impl_array_two_offsets!(GeometryCollectionArray<O1>, GeometryCollectionArray<O2>);

impl Intersects for &dyn GeometryArrayTrait {
    type Output = (UInt32Array, UInt32Array);

    fn intersects_indices(&self, rhs: &Self) -> Self::Output {
        use GeoDataType::*;
        match (self.data_type(), rhs.data_type()) {
            // Point implementations
            (Point(_), Point(_)) => Intersects::intersects_indices(self.as_point(), rhs.as_point()),
            (Point(_), LineString(_)) => Intersects::intersects_indices(self.as_point(), rhs.as_line_string()),
            (Point(_), LargeLineString(_)) => Intersects::intersects_indices(self.as_point(), rhs.as_large_line_string()),
            (Point(_), Polygon(_)) => Intersects::intersects_indices(self.as_point(), rhs.as_polygon()),
            (Point(_), LargePolygon(_)) => Intersects::intersects_indices(self.as_point(), rhs.as_large_polygon()),
            (Point(_), MultiPoint(_)) => Intersects::intersects_indices(self.as_point(), rhs.as_multi_point()),
            (Point(_), LargeMultiPoint(_)) => Intersects::intersects_indices(self.as_point(), rhs.as_large_multi_point()),
            (Point(_), MultiLineString(_)) => Intersects::intersects_indices(self.as_point(), rhs.as_multi_line_string()),
            (Point(_), LargeMultiLineString(_)) => Intersects::intersects_indices(self.as_point(), rhs.as_large_multi_line_string()),
            (Point(_), MultiPolygon(_)) => Intersects::intersects_indices(self.as_point(), rhs.as_multi_polygon()),
            (Point(_), LargeMultiPolygon(_)) => Intersects::intersects_indices(self.as_point(), rhs.as_large_multi_polygon()),
            (Point(_), Mixed(_)) => Intersects::intersects_indices(self.as_point(), rhs.as_mixed()),
            (Point(_), LargeMixed(_)) => Intersects::intersects_indices(self.as_point(), rhs.as_large_mixed()),
            (Point(_), GeometryCollection(_)) => Intersects::intersects_indices(self.as_point(), rhs.as_geometry_collection()),
            (Point(_), LargeGeometryCollection(_)) => Intersects::intersects_indices(self.as_point(), rhs.as_large_geometry_collection()),

            // Reverse implementations on Point
            (LineString(_), Point(_)) => Intersects::intersects_indices(rhs.as_line_string(), self.as_point()),
            (LargeLineString(_), Point(_)) => Intersects::intersects_indices(rhs.as_large_line_string(), self.as_point()),
            (Polygon(_), Point(_)) => Intersects::intersects_indices(rhs.as_polygon(), self.as_point()),
            (LargePolygon(_), Point(_)) => Intersects::intersects_indices(rhs.as_large_polygon(), self.as_point()),
            (MultiPoint(_), Point(_)) => Intersects::intersects_indices(rhs.as_multi_point(), self.as_point()),
            (LargeMultiPoint(_), Point(_)) => Intersects::intersects_indices(rhs.as_large_multi_point(), self.as_point()),
            (MultiLineString(_), Point(_)) => Intersects::intersects_indices(rhs.as_multi_line_string(), self.as_point()),
            (LargeMultiLineString(_), Point(_)) => Intersects::intersects_indices(rhs.as_large_multi_line_string(), self.as_point()),
            (MultiPolygon(_), Point(_)) => Intersects::intersects_indices(rhs.as_multi_polygon(), self.as_point()),
            (LargeMultiPolygon(_), Point(_)) => Intersects::intersects_indices(rhs.as_large_multi_polygon(), self.as_point()),
            (Mixed(_), Point(_)) => Intersects::intersects_indices(rhs.as_mixed(), self.as_point()),
            (LargeMixed(_), Point(_)) => Intersects::intersects_indices(rhs.as_large_mixed(), self.as_point()),
            (GeometryCollection(_), Point(_)) => Intersects::intersects_indices(rhs.as_geometry_collection(), self.as_point()),
            (LargeGeometryCollection(_), Point(_)) => Intersects::intersects_indices(rhs.as_large_geometry_collection(), self.as_point()),

            // Implementations on LineStringArray
            (LineString(_), LineString(_)) => Intersects::intersects_indices(self.as_line_string(), rhs.as_line_string()),
            (LineString(_), Polygon(_)) => Intersects::intersects_indices(self.as_line_string(), rhs.as_polygon()),
            (LineString(_), MultiPoint(_)) => Intersects::intersects_indices(self.as_line_string(), rhs.as_multi_point()),
            (LineString(_), MultiLineString(_)) => Intersects::intersects_indices(self.as_line_string(), rhs.as_multi_line_string()),
            (LineString(_), MultiPolygon(_)) => Intersects::intersects_indices(self.as_line_string(), rhs.as_multi_polygon()),
            (LineString(_), Mixed(_)) => Intersects::intersects_indices(self.as_line_string(), rhs.as_mixed()),
            (LineString(_), GeometryCollection(_)) => Intersects::intersects_indices(self.as_line_string(), rhs.as_geometry_collection()),

            (Polygon(_), LineString(_)) => Intersects::intersects_indices(rhs.as_line_string(), self.as_polygon()),
            (MultiPoint(_), LineString(_)) => Intersects::intersects_indices(rhs.as_line_string(), self.as_multi_point()),
            (MultiLineString(_), LineString(_)) => Intersects::intersects_indices(rhs.as_line_string(), self.as_multi_line_string()),
            (MultiPolygon(_), LineString(_)) => Intersects::intersects_indices(rhs.as_line_string(), self.as_multi_polygon()),
            (Mixed(_), LineString(_)) => Intersects::intersects_indices(rhs.as_line_string(), self.as_mixed()),
            (GeometryCollection(_), LineString(_)) => Intersects::intersects_indices(rhs.as_line_string(), self.as_geometry_collection()),

            (LargeLineString(_), LineString(_)) => Intersects::intersects_indices(self.as_large_line_string(), rhs.as_line_string()),
            (LargeLineString(_), Polygon(_)) => Intersects::intersects_indices(self.as_large_line_string(), rhs.as_polygon()),
            (LargeLineString(_), MultiPoint(_)) => Intersects::intersects_indices(self.as_large_line_string(), rhs.as_multi_point()),
            (LargeLineString(_), MultiLineString(_)) => Intersects::intersects_indices(self.as_large_line_string(), rhs.as_multi_line_string()),
            (LargeLineString(_), MultiPolygon(_)) => Intersects::intersects_indices(self.as_large_line_string(), rhs.as_multi_polygon()),
            (LargeLineString(_), Mixed(_)) => Intersects::intersects_indices(self.as_large_line_string(), rhs.as_mixed()),
            (LargeLineString(_), GeometryCollection(_)) => Intersects::intersects_indices(self.as_large_line_string(), rhs.as_geometry_collection()),

            (Polygon(_), LargeLineString(_)) => Intersects::intersects_indices(rhs.as_large_line_string(), self.as_polygon()),
            (MultiPoint(_), LargeLineString(_)) => Intersects::intersects_indices(rhs.as_large_line_string(), self.as_multi_point()),
            (MultiLineString(_), LargeLineString(_)) => Intersects::intersects_indices(rhs.as_large_line_string(), self.as_multi_line_string()),
            (MultiPolygon(_), LargeLineString(_)) => Intersects::intersects_indices(rhs.as_large_line_string(), self.as_multi_polygon()),
            (Mixed(_), LargeLineString(_)) => Intersects::intersects_indices(rhs.as_large_line_string(), self.as_mixed()),
            (GeometryCollection(_), LargeLineString(_)) => Intersects::intersects_indices(rhs.as_large_line_string(), self.as_geometry_collection()),

            (LargeLineString(_), LargeLineString(_)) => Intersects::intersects_indices(self.as_large_line_string(), rhs.as_large_line_string()),
            (LargeLineString(_), LargePolygon(_)) => Intersects::intersects_indices(self.as_large_line_string(), rhs.as_large_polygon()),
            (LargeLineString(_), LargeMultiPoint(_)) => Intersects::intersects_indices(self.as_large_line_string(), rhs.as_large_multi_point()),
            (LargeLineString(_), LargeMultiLineString(_)) => Intersects::intersects_indices(self.as_large_line_string(), rhs.as_large_multi_line_string()),
            (LargeLineString(_), LargeMultiPolygon(_)) => Intersects::intersects_indices(self.as_large_line_string(), rhs.as_large_multi_polygon()),
            (LargeLineString(_), LargeMixed(_)) => Intersects::intersects_indices(self.as_large_line_string(), rhs.as_large_mixed()),
            (LargeLineString(_), LargeGeometryCollection(_)) => Intersects::intersects_indices(self.as_large_line_string(), rhs.as_large_geometry_collection()),

            (LargePolygon(_), LargeLineString(_)) => Intersects::intersects_indices(rhs.as_large_line_string(), self.as_large_polygon()),
            (LargeMultiPoint(_), LargeLineString(_)) => Intersects::intersects_indices(rhs.as_large_line_string(), self.as_large_multi_point()),
            (LargeMultiLineString(_), LargeLineString(_)) => Intersects::intersects_indices(rhs.as_large_line_string(), self.as_large_multi_line_string()),
            (LargeMultiPolygon(_), LargeLineString(_)) => Intersects::intersects_indices(rhs.as_large_line_string(), self.as_large_multi_polygon()),
            (LargeMixed(_), LargeLineString(_)) => Intersects::intersects_indices(rhs.as_large_line_string(), self.as_large_mixed()),
            (LargeGeometryCollection(_), LargeLineString(_)) => Intersects::intersects_indices(rhs.as_large_line_string(), self.as_large_geometry_collection()),

            // Resume here!

            // Implementations on PolygonArray
            (Polygon(_), Polygon(_)) => Intersects::intersects_indices(self.as_polygon(), rhs.as_polygon()),
            (Polygon(_), MultiPoint(_)) => Intersects::intersects_indices(self.as_polygon(), rhs.as_multi_point()),
            (Polygon(_), MultiLineString(_)) => Intersects::intersects_indices(self.as_polygon(), rhs.as_multi_line_string()),
            (Polygon(_), MultiPolygon(_)) => Intersects::intersects_indices(self.as_polygon(), rhs.as_multi_polygon()),
            (Polygon(_), Mixed(_)) => Intersects::intersects_indices(self.as_polygon(), rhs.as_mixed()),
            (Polygon(_), GeometryCollection(_)) => Intersects::intersects_indices(self.as_polygon(), rhs.as_geometry_collection()),

            (MultiPoint(_), Polygon(_)) => Intersects::intersects_indices(rhs.as_polygon(), self.as_multi_point()),
            (MultiLineString(_), Polygon(_)) => Intersects::intersects_indices(rhs.as_polygon(), self.as_multi_line_string()),
            (MultiPolygon(_), Polygon(_)) => Intersects::intersects_indices(rhs.as_polygon(), self.as_multi_polygon()),
            (Mixed(_), Polygon(_)) => Intersects::intersects_indices(rhs.as_polygon(), self.as_mixed()),
            (GeometryCollection(_), Polygon(_)) => Intersects::intersects_indices(rhs.as_polygon(), self.as_geometry_collection()),

// Implementations on MultiPointArray
            (MultiPoint(_), MultiPoint(_)) => Intersects::intersects_indices(self.as_multi_point(), rhs.as_multi_point()),
            (MultiPoint(_), MultiLineString(_)) => Intersects::intersects_indices(self.as_multi_point(), rhs.as_multi_line_string()),
            (MultiPoint(_), MultiPolygon(_)) => Intersects::intersects_indices(self.as_multi_point(), rhs.as_multi_polygon()),
            (MultiPoint(_), Mixed(_)) => Intersects::intersects_indices(self.as_multi_point(), rhs.as_mixed()),
            (MultiPoint(_), GeometryCollection(_)) => Intersects::intersects_indices(self.as_multi_point(), rhs.as_geometry_collection()),

            (MultiLineString(_), MultiPoint(_)) => Intersects::intersects_indices(rhs.as_multi_point(), self.as_multi_line_string()),
            (MultiPolygon(_), MultiPoint(_)) => Intersects::intersects_indices(rhs.as_multi_point(), self.as_multi_polygon()),
            (Mixed(_), MultiPoint(_)) => Intersects::intersects_indices(rhs.as_multi_point(), self.as_mixed()),
            (GeometryCollection(_), MultiPoint(_)) => Intersects::intersects_indices(rhs.as_multi_point(), self.as_geometry_collection()),

// Implementations on MultiLineStringArray
            (MultiLineString(_), MultiLineString(_)) => Intersects::intersects_indices(self.as_multi_line_string(), rhs.as_multi_line_string()),
            (MultiLineString(_), MultiPolygon(_)) => Intersects::intersects_indices(self.as_multi_line_string(), rhs.as_multi_polygon()),
            (MultiLineString(_), Mixed(_)) => Intersects::intersects_indices(self.as_multi_line_string(), rhs.as_mixed()),
            (MultiLineString(_), GeometryCollection(_)) => Intersects::intersects_indices(self.as_multi_line_string(), rhs.as_geometry_collection()),

            (MultiPolygon(_), MultiLineString(_)) => Intersects::intersects_indices(rhs.as_multi_line_string(), self.as_multi_polygon()),
            (Mixed(_), MultiLineString(_)) => Intersects::intersects_indices(rhs.as_multi_line_string(), self.as_mixed()),
            (GeometryCollection(_), MultiLineString(_)) => Intersects::intersects_indices(rhs.as_multi_line_string(), self.as_geometry_collection()),

// Implementations on MultiPolygonArray
            (MultiPolygon(_), MultiPolygon(_)) => Intersects::intersects_indices(self.as_multi_polygon(), rhs.as_multi_polygon()),
            (MultiPolygon(_), Mixed(_)) => Intersects::intersects_indices(self.as_multi_polygon(), rhs.as_mixed()),
            (MultiPolygon(_), GeometryCollection(_)) => Intersects::intersects_indices(self.as_multi_polygon(), rhs.as_geometry_collection()),

            (Mixed(_), MultiPolygon(_)) => Intersects::intersects_indices(rhs.as_multi_polygon(), self.as_mixed()),
            (GeometryCollection(_), MultiPolygon(_)) => Intersects::intersects_indices(rhs.as_multi_polygon(), self.as_geometry_collection()),

// Implementations on MixedGeometryArray
            (Mixed(_), Mixed(_)) => Intersects::intersects_indices(self.as_mixed(), rhs.as_mixed()),
            (Mixed(_), GeometryCollection(_)) => Intersects::intersects_indices(self.as_mixed(), rhs.as_geometry_collection()),

            (GeometryCollection(_), Mixed(_)) => Intersects::intersects_indices(rhs.as_mixed(), self.as_geometry_collection()),

// Implementations on GeometryCollectionArray
            (GeometryCollection(_), GeometryCollection(_)) => Intersects::intersects_indices(self.as_geometry_collection(), rhs.as_geometry_collection()),

        }
    }
}
