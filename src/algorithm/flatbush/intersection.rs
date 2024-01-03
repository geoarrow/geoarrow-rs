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

    #[rustfmt::skip]
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

            // Implementations on LineStringArray
            (LineString(_), LineString(_)) => Intersects::intersects_indices(self.as_line_string(), rhs.as_line_string()),
            (LineString(_), LargeLineString(_)) => Intersects::intersects_indices(self.as_line_string(), rhs.as_large_line_string()),
            (LineString(_), Polygon(_)) => Intersects::intersects_indices(self.as_line_string(), rhs.as_polygon()),
            (LineString(_), LargePolygon(_)) => Intersects::intersects_indices(self.as_line_string(), rhs.as_large_polygon()),
            (LineString(_), MultiPoint(_)) => Intersects::intersects_indices(self.as_line_string(), rhs.as_multi_point()),
            (LineString(_), LargeMultiPoint(_)) => Intersects::intersects_indices(self.as_line_string(), rhs.as_large_multi_point()),
            (LineString(_), MultiLineString(_)) => Intersects::intersects_indices(self.as_line_string(), rhs.as_multi_line_string()),
            (LineString(_), LargeMultiLineString(_)) => Intersects::intersects_indices(self.as_line_string(), rhs.as_large_multi_line_string()),
            (LineString(_), MultiPolygon(_)) => Intersects::intersects_indices(self.as_line_string(), rhs.as_multi_polygon()),
            (LineString(_), LargeMultiPolygon(_)) => Intersects::intersects_indices(self.as_line_string(), rhs.as_large_multi_polygon()),
            (LineString(_), Mixed(_)) => Intersects::intersects_indices(self.as_line_string(), rhs.as_mixed()),
            (LineString(_), LargeMixed(_)) => Intersects::intersects_indices(self.as_line_string(), rhs.as_large_mixed()),
            (LineString(_), GeometryCollection(_)) => Intersects::intersects_indices(self.as_line_string(), rhs.as_geometry_collection()),
            (LineString(_), LargeGeometryCollection(_)) => Intersects::intersects_indices(self.as_line_string(), rhs.as_large_geometry_collection()),

            // Implementations on LargeLineStringArray
            (LargeLineString(_), LargeLineString(_)) => Intersects::intersects_indices(self.as_large_line_string(), rhs.as_large_line_string()),
            (LargeLineString(_), Polygon(_)) => Intersects::intersects_indices(self.as_large_line_string(), rhs.as_polygon()),
            (LargeLineString(_), LargePolygon(_)) => Intersects::intersects_indices(self.as_large_line_string(), rhs.as_large_polygon()),
            (LargeLineString(_), MultiPoint(_)) => Intersects::intersects_indices(self.as_large_line_string(), rhs.as_multi_point()),
            (LargeLineString(_), LargeMultiPoint(_)) => Intersects::intersects_indices(self.as_large_line_string(), rhs.as_large_multi_point()),
            (LargeLineString(_), MultiLineString(_)) => Intersects::intersects_indices(self.as_large_line_string(), rhs.as_multi_line_string()),
            (LargeLineString(_), LargeMultiLineString(_)) => Intersects::intersects_indices(self.as_large_line_string(), rhs.as_large_multi_line_string()),
            (LargeLineString(_), MultiPolygon(_)) => Intersects::intersects_indices(self.as_large_line_string(), rhs.as_multi_polygon()),
            (LargeLineString(_), LargeMultiPolygon(_)) => Intersects::intersects_indices(self.as_large_line_string(), rhs.as_large_multi_polygon()),
            (LargeLineString(_), Mixed(_)) => Intersects::intersects_indices(self.as_large_line_string(), rhs.as_mixed()),
            (LargeLineString(_), LargeMixed(_)) => Intersects::intersects_indices(self.as_large_line_string(), rhs.as_large_mixed()),
            (LargeLineString(_), GeometryCollection(_)) => Intersects::intersects_indices(self.as_large_line_string(), rhs.as_geometry_collection()),
            (LargeLineString(_), LargeGeometryCollection(_)) => Intersects::intersects_indices(self.as_large_line_string(), rhs.as_large_geometry_collection()),

            // Implementations on PolygonArray
            (Polygon(_), Polygon(_)) => Intersects::intersects_indices(self.as_polygon(), rhs.as_polygon()),
            (Polygon(_), LargePolygon(_)) => Intersects::intersects_indices(self.as_polygon(), rhs.as_large_polygon()),
            (Polygon(_), MultiPoint(_)) => Intersects::intersects_indices(self.as_polygon(), rhs.as_multi_point()),
            (Polygon(_), LargeMultiPoint(_)) => Intersects::intersects_indices(self.as_polygon(), rhs.as_large_multi_point()),
            (Polygon(_), MultiLineString(_)) => Intersects::intersects_indices(self.as_polygon(), rhs.as_multi_line_string()),
            (Polygon(_), LargeMultiLineString(_)) => Intersects::intersects_indices(self.as_polygon(), rhs.as_large_multi_line_string()),
            (Polygon(_), MultiPolygon(_)) => Intersects::intersects_indices(self.as_polygon(), rhs.as_multi_polygon()),
            (Polygon(_), LargeMultiPolygon(_)) => Intersects::intersects_indices(self.as_polygon(), rhs.as_large_multi_polygon()),
            (Polygon(_), Mixed(_)) => Intersects::intersects_indices(self.as_polygon(), rhs.as_mixed()),
            (Polygon(_), LargeMixed(_)) => Intersects::intersects_indices(self.as_polygon(), rhs.as_large_mixed()),
            (Polygon(_), GeometryCollection(_)) => Intersects::intersects_indices(self.as_polygon(), rhs.as_geometry_collection()),
            (Polygon(_), LargeGeometryCollection(_)) => Intersects::intersects_indices(self.as_polygon(), rhs.as_large_geometry_collection()),

            // Implementations on LargePolygon
            (LargePolygon(_), LargePolygon(_)) => Intersects::intersects_indices(self.as_large_polygon(), rhs.as_large_polygon()),
            (LargePolygon(_), MultiPoint(_)) => Intersects::intersects_indices(self.as_large_polygon(), rhs.as_multi_point()),
            (LargePolygon(_), LargeMultiPoint(_)) => Intersects::intersects_indices(self.as_large_polygon(), rhs.as_large_multi_point()),
            (LargePolygon(_), MultiLineString(_)) => Intersects::intersects_indices(self.as_large_polygon(), rhs.as_multi_line_string()),
            (LargePolygon(_), LargeMultiLineString(_)) => Intersects::intersects_indices(self.as_large_polygon(), rhs.as_large_multi_line_string()),
            (LargePolygon(_), MultiPolygon(_)) => Intersects::intersects_indices(self.as_large_polygon(), rhs.as_multi_polygon()),
            (LargePolygon(_), LargeMultiPolygon(_)) => Intersects::intersects_indices(self.as_large_polygon(), rhs.as_large_multi_polygon()),
            (LargePolygon(_), Mixed(_)) => Intersects::intersects_indices(self.as_large_polygon(), rhs.as_mixed()),
            (LargePolygon(_), LargeMixed(_)) => Intersects::intersects_indices(self.as_large_polygon(), rhs.as_large_mixed()),
            (LargePolygon(_), GeometryCollection(_)) => Intersects::intersects_indices(self.as_large_polygon(), rhs.as_geometry_collection()),
            (LargePolygon(_), LargeGeometryCollection(_)) => Intersects::intersects_indices(self.as_large_polygon(), rhs.as_large_geometry_collection()),

            // Implementations on MultiPoint
            (MultiPoint(_), MultiPoint(_)) => Intersects::intersects_indices(self.as_multi_point(), rhs.as_multi_point()),
            (MultiPoint(_), LargeMultiPoint(_)) => Intersects::intersects_indices(self.as_multi_point(), rhs.as_large_multi_point()),
            (MultiPoint(_), MultiLineString(_)) => Intersects::intersects_indices(self.as_multi_point(), rhs.as_multi_line_string()),
            (MultiPoint(_), LargeMultiLineString(_)) => Intersects::intersects_indices(self.as_multi_point(), rhs.as_large_multi_line_string()),
            (MultiPoint(_), MultiPolygon(_)) => Intersects::intersects_indices(self.as_multi_point(), rhs.as_multi_polygon()),
            (MultiPoint(_), LargeMultiPolygon(_)) => Intersects::intersects_indices(self.as_multi_point(), rhs.as_large_multi_polygon()),
            (MultiPoint(_), Mixed(_)) => Intersects::intersects_indices(self.as_multi_point(), rhs.as_mixed()),
            (MultiPoint(_), LargeMixed(_)) => Intersects::intersects_indices(self.as_multi_point(), rhs.as_large_mixed()),
            (MultiPoint(_), GeometryCollection(_)) => Intersects::intersects_indices(self.as_multi_point(), rhs.as_geometry_collection()),
            (MultiPoint(_), LargeGeometryCollection(_)) => Intersects::intersects_indices(self.as_multi_point(), rhs.as_large_geometry_collection()),

            // Implementations on LargeMultiPoint
            (LargeMultiPoint(_), LargeMultiPoint(_)) => Intersects::intersects_indices(self.as_large_multi_point(), rhs.as_large_multi_point()),
            (LargeMultiPoint(_), MultiLineString(_)) => Intersects::intersects_indices(self.as_large_multi_point(), rhs.as_multi_line_string()),
            (LargeMultiPoint(_), LargeMultiLineString(_)) => Intersects::intersects_indices(self.as_large_multi_point(), rhs.as_large_multi_line_string()),
            (LargeMultiPoint(_), MultiPolygon(_)) => Intersects::intersects_indices(self.as_large_multi_point(), rhs.as_multi_polygon()),
            (LargeMultiPoint(_), LargeMultiPolygon(_)) => Intersects::intersects_indices(self.as_large_multi_point(), rhs.as_large_multi_polygon()),
            (LargeMultiPoint(_), Mixed(_)) => Intersects::intersects_indices(self.as_large_multi_point(), rhs.as_mixed()),
            (LargeMultiPoint(_), LargeMixed(_)) => Intersects::intersects_indices(self.as_large_multi_point(), rhs.as_large_mixed()),
            (LargeMultiPoint(_), GeometryCollection(_)) => Intersects::intersects_indices(self.as_large_multi_point(), rhs.as_geometry_collection()),
            (LargeMultiPoint(_), LargeGeometryCollection(_)) => Intersects::intersects_indices(self.as_large_multi_point(), rhs.as_large_geometry_collection()),

            // Implementations on MultiLineString
            (MultiLineString(_), MultiLineString(_)) => Intersects::intersects_indices(self.as_multi_line_string(), rhs.as_multi_line_string()),
            (MultiLineString(_), LargeMultiLineString(_)) => Intersects::intersects_indices(self.as_multi_line_string(), rhs.as_large_multi_line_string()),
            (MultiLineString(_), MultiPolygon(_)) => Intersects::intersects_indices(self.as_multi_line_string(), rhs.as_multi_polygon()),
            (MultiLineString(_), LargeMultiPolygon(_)) => Intersects::intersects_indices(self.as_multi_line_string(), rhs.as_large_multi_polygon()),
            (MultiLineString(_), Mixed(_)) => Intersects::intersects_indices(self.as_multi_line_string(), rhs.as_mixed()),
            (MultiLineString(_), LargeMixed(_)) => Intersects::intersects_indices(self.as_multi_line_string(), rhs.as_large_mixed()),
            (MultiLineString(_), GeometryCollection(_)) => Intersects::intersects_indices(self.as_multi_line_string(), rhs.as_geometry_collection()),
            (MultiLineString(_), LargeGeometryCollection(_)) => Intersects::intersects_indices(self.as_multi_line_string(), rhs.as_large_geometry_collection()),

            // Implementations on LargeMultiLineString
            (LargeMultiLineString(_), LargeMultiLineString(_)) => Intersects::intersects_indices(self.as_large_multi_line_string(), rhs.as_large_multi_line_string()),
            (LargeMultiLineString(_), MultiPolygon(_)) => Intersects::intersects_indices(self.as_large_multi_line_string(), rhs.as_multi_polygon()),
            (LargeMultiLineString(_), LargeMultiPolygon(_)) => Intersects::intersects_indices(self.as_large_multi_line_string(), rhs.as_large_multi_polygon()),
            (LargeMultiLineString(_), Mixed(_)) => Intersects::intersects_indices(self.as_large_multi_line_string(), rhs.as_mixed()),
            (LargeMultiLineString(_), LargeMixed(_)) => Intersects::intersects_indices(self.as_large_multi_line_string(), rhs.as_large_mixed()),
            (LargeMultiLineString(_), GeometryCollection(_)) => Intersects::intersects_indices(self.as_large_multi_line_string(), rhs.as_geometry_collection()),
            (LargeMultiLineString(_), LargeGeometryCollection(_)) => Intersects::intersects_indices(self.as_large_multi_line_string(), rhs.as_large_geometry_collection()),

            // Implementations on MultiPolygon
            (MultiPolygon(_), MultiPolygon(_)) => Intersects::intersects_indices(self.as_multi_polygon(), rhs.as_multi_polygon()),
            (MultiPolygon(_), LargeMultiPolygon(_)) => Intersects::intersects_indices(self.as_multi_polygon(), rhs.as_large_multi_polygon()),
            (MultiPolygon(_), Mixed(_)) => Intersects::intersects_indices(self.as_multi_polygon(), rhs.as_mixed()),
            (MultiPolygon(_), LargeMixed(_)) => Intersects::intersects_indices(self.as_multi_polygon(), rhs.as_large_mixed()),
            (MultiPolygon(_), GeometryCollection(_)) => Intersects::intersects_indices(self.as_multi_polygon(), rhs.as_geometry_collection()),
            (MultiPolygon(_), LargeGeometryCollection(_)) => Intersects::intersects_indices(self.as_multi_polygon(), rhs.as_large_geometry_collection()),

            // Implementations on LargeMultiPolygon
            (LargeMultiPolygon(_), LargeMultiPolygon(_)) => Intersects::intersects_indices(self.as_large_multi_polygon(), rhs.as_large_multi_polygon()),
            (LargeMultiPolygon(_), Mixed(_)) => Intersects::intersects_indices(self.as_large_multi_polygon(), rhs.as_mixed()),
            (LargeMultiPolygon(_), LargeMixed(_)) => Intersects::intersects_indices(self.as_large_multi_polygon(), rhs.as_large_mixed()),
            (LargeMultiPolygon(_), GeometryCollection(_)) => Intersects::intersects_indices(self.as_large_multi_polygon(), rhs.as_geometry_collection()),
            (LargeMultiPolygon(_), LargeGeometryCollection(_)) => Intersects::intersects_indices(self.as_large_multi_polygon(), rhs.as_large_geometry_collection()),

            // Implementations on Mixed
            (Mixed(_), Mixed(_)) => Intersects::intersects_indices(self.as_mixed(), rhs.as_mixed()),
            (Mixed(_), LargeMixed(_)) => Intersects::intersects_indices(self.as_mixed(), rhs.as_large_mixed()),
            (Mixed(_), GeometryCollection(_)) => Intersects::intersects_indices(self.as_mixed(), rhs.as_geometry_collection()),
            (Mixed(_), LargeGeometryCollection(_)) => Intersects::intersects_indices(self.as_mixed(), rhs.as_large_geometry_collection()),

            // Implementations on LargeMixed
            (LargeMixed(_), LargeMixed(_)) => Intersects::intersects_indices(self.as_large_mixed(), rhs.as_large_mixed()),
            (LargeMixed(_), GeometryCollection(_)) => Intersects::intersects_indices(self.as_large_mixed(), rhs.as_geometry_collection()),
            (LargeMixed(_), LargeGeometryCollection(_)) => Intersects::intersects_indices(self.as_large_mixed(), rhs.as_large_geometry_collection()),

            // Implementations on GeometryCollection
            (GeometryCollection(_), GeometryCollection(_)) => Intersects::intersects_indices(self.as_geometry_collection(), rhs.as_geometry_collection()),
            (GeometryCollection(_), LargeGeometryCollection(_)) => Intersects::intersects_indices(self.as_geometry_collection(), rhs.as_large_geometry_collection()),

            // Implementations on LargeGeometryCollection
            (LargeGeometryCollection(_), LargeGeometryCollection(_)) => Intersects::intersects_indices(self.as_large_geometry_collection(), rhs.as_large_geometry_collection()),

            // Reverse implementations
            (_, Point(_)) => Intersects::intersects_indices(rhs, self),
            (_, LineString(_)) => Intersects::intersects_indices(rhs, self),
            (_, LargeLineString(_)) => Intersects::intersects_indices(rhs, self),
            (_, Polygon(_)) => Intersects::intersects_indices(rhs, self),
            (_, LargePolygon(_)) => Intersects::intersects_indices(rhs, self),
            (_, MultiPoint(_)) => Intersects::intersects_indices(rhs, self),
            (_, LargeMultiPoint(_)) => Intersects::intersects_indices(rhs, self),
            (_, MultiLineString(_)) => Intersects::intersects_indices(rhs, self),
            (_, LargeMultiLineString(_)) => Intersects::intersects_indices(rhs, self),
            (_, MultiPolygon(_)) => Intersects::intersects_indices(rhs, self),
            (_, LargeMultiPolygon(_)) => Intersects::intersects_indices(rhs, self),
            (_, Mixed(_)) => Intersects::intersects_indices(rhs, self),
            (_, LargeMixed(_)) => Intersects::intersects_indices(rhs, self),
            (_, GeometryCollection(_)) => Intersects::intersects_indices(rhs, self),
            (_, LargeGeometryCollection(_)) => Intersects::intersects_indices(rhs, self),
            _ => unimplemented!(),
            // (_, WKB) => Intersects::intersects_indices(rhs, self),
            // (_, LargeWKB) => Intersects::intersects_indices(rhs, self),
            // (_, Rect) => Intersects::intersects_indices(rhs, self),
        }
    }
}
