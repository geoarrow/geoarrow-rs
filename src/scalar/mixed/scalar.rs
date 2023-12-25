use crate::geo_traits::GeometryType;
use crate::scalar::*;
use crate::trait_::GeometryArrayAccessor;
use std::borrow::Cow;

use arrow_array::OffsetSizeTrait;

use crate::array::MixedGeometryArray;
use crate::geo_traits::GeometryTrait;

/// An Arrow equivalent of a mixed Geometry
#[derive(Debug, Clone)]
pub struct MixedGeometry<'a, O: OffsetSizeTrait> {
    pub(crate) arr: Cow<'a, MixedGeometryArray<O>>,

    pub(crate) geom_index: usize,
}

impl<'a, O: OffsetSizeTrait> MixedGeometry<'a, O> {
    pub fn new_borrowed(arr: &'a MixedGeometryArray<O>, geom_index: usize) -> Self {
        Self {
            arr: Cow::Borrowed(arr),
            geom_index,
        }
    }
}

impl<'a, O: OffsetSizeTrait> GeometryTrait for MixedGeometry<'a, O> {
    type T = f64;
    type Point<'b> = Point<'b> where Self: 'b;
    type LineString<'b> = LineString<'b, O> where Self: 'b;
    type Polygon<'b> = Polygon<'b, O> where Self: 'b;
    type MultiPoint<'b> = MultiPoint<'b, O> where Self: 'b;
    type MultiLineString<'b> = MultiLineString<'b, O> where Self: 'b;
    type MultiPolygon<'b> = MultiPolygon<'b, O> where Self: 'b;
    type GeometryCollection<'b> = GeometryCollection<'b, O> where Self: 'b;
    type Rect<'b> = Rect<'b> where Self: 'b;

    fn as_type(
        &self,
    ) -> crate::geo_traits::GeometryType<
        '_,
        Point<'_>,
        LineString<'_, O>,
        Polygon<'_, O>,
        MultiPoint<'_, O>,
        MultiLineString<'_, O>,
        MultiPolygon<'_, O>,
        GeometryCollection<'_, O>,
        Rect<'_>,
    > {
        let geom = self.arr.value(self.geom_index);
        geom.as_type()

        // let child_index = self.arr.type_ids[self.geom_index];
        // let offset = self.arr.offsets[self.geom_index] as usize;
        // let geometry_type = self.arr.map[child_index as usize].unwrap();

        // match geometry_type {
        //     crate::array::mixed::GeometryType::Point => {
        //         GeometryType::Point(&self.arr.points.as_ref().unwrap().value(offset))
        //     }
        //     crate::array::mixed::GeometryType::LineString => {
        //         GeometryType::LineString(&self.arr.line_strings.as_ref().unwrap().value(offset))
        //     }
        //     crate::array::mixed::GeometryType::Polygon => {
        //         GeometryType::Polygon(&self.arr.polygons.as_ref().unwrap().value(offset))
        //     }
        //     crate::array::mixed::GeometryType::MultiPoint => {
        //         GeometryType::MultiPoint(&self.arr.multi_points.as_ref().unwrap().value(offset))
        //     }
        //     crate::array::mixed::GeometryType::MultiLineString => {
        //         GeometryType::MultiLineString(&self.arr.multi_line_strings.as_ref().unwrap().value(offset))
        //     }
        //     crate::array::mixed::GeometryType::MultiPolygon => {
        //         GeometryType::MultiPolygon(&self.arr.multi_polygons.as_ref().unwrap().value(offset))
        //     }
        // }
    }
}
