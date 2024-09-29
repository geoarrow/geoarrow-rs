use std::sync::Arc;

use crate::algorithm::native::Downcast;
use crate::array::geometrycollection::GeometryCollectionBuilder;
use crate::array::*;
use crate::chunked_array::*;
use crate::datatypes::{Dimension, NativeType};
use crate::error::{GeoArrowError, Result};
use crate::scalar::WKB;
use crate::trait_::ArrayAccessor;
use crate::NativeArray;
use arrow_array::OffsetSizeTrait;

/// An optimized implementation of converting from ISO WKB-encoded geometries.
///
/// This implementation performs a two-pass approach, first scanning the input geometries to
/// determine the exact buffer sizes, then making a single set of allocations and filling those new
/// arrays with the WKB coordinate values.
pub trait FromWKB: Sized {
    type Input<O: OffsetSizeTrait>;

    fn from_wkb<O: OffsetSizeTrait>(arr: &Self::Input<O>, coord_type: CoordType) -> Result<Self>;
}

impl FromWKB for PointArray<2> {
    type Input<O: OffsetSizeTrait> = WKBArray<O>;

    fn from_wkb<O: OffsetSizeTrait>(arr: &WKBArray<O>, coord_type: CoordType) -> Result<Self> {
        let wkb_objects: Vec<Option<WKB<'_, O>>> = arr.iter().collect();
        let builder = PointBuilder::from_wkb(&wkb_objects, Some(coord_type), arr.metadata())?;
        Ok(builder.finish())
    }
}

macro_rules! impl_from_wkb {
    ($array:ty, $builder:ty) => {
        impl FromWKB for $array {
            type Input<O: OffsetSizeTrait> = WKBArray<O>;

            fn from_wkb<O: OffsetSizeTrait>(arr: &WKBArray<O>, coord_type: CoordType) -> Result<Self> {
                let wkb_objects: Vec<Option<WKB<'_, O>>> = arr.iter().collect();
                let builder = <$builder>::from_wkb(&wkb_objects, Some(coord_type), arr.metadata())?;
                Ok(builder.finish())
            }
        }
    };
}

impl_from_wkb!(LineStringArray<2>, LineStringBuilder<2>);
impl_from_wkb!(PolygonArray<2>, PolygonBuilder<2>);
impl_from_wkb!(MultiPointArray<2>, MultiPointBuilder<2>);
impl_from_wkb!(MultiLineStringArray<2>, MultiLineStringBuilder<2>);
impl_from_wkb!(MultiPolygonArray<2>, MultiPolygonBuilder<2>);

impl FromWKB for MixedGeometryArray<2> {
    type Input<O: OffsetSizeTrait> = WKBArray<O>;

    fn from_wkb<O: OffsetSizeTrait>(arr: &WKBArray<O>, coord_type: CoordType) -> Result<Self> {
        let wkb_objects: Vec<Option<WKB<'_, O>>> = arr.iter().collect();
        let builder = MixedGeometryBuilder::<2>::from_wkb(&wkb_objects, Some(coord_type), arr.metadata(), true)?;
        Ok(builder.finish())
    }
}

impl FromWKB for GeometryCollectionArray<2> {
    type Input<O: OffsetSizeTrait> = WKBArray<O>;

    fn from_wkb<O: OffsetSizeTrait>(arr: &WKBArray<O>, coord_type: CoordType) -> Result<Self> {
        let wkb_objects: Vec<Option<WKB<'_, O>>> = arr.iter().collect();
        let builder = GeometryCollectionBuilder::<2>::from_wkb(&wkb_objects, Some(coord_type), arr.metadata(), true)?;
        Ok(builder.finish())
    }
}

impl FromWKB for Arc<dyn NativeArray> {
    type Input<O: OffsetSizeTrait> = WKBArray<O>;

    fn from_wkb<O: OffsetSizeTrait>(arr: &WKBArray<O>, coord_type: CoordType) -> Result<Self> {
        let wkb_objects: Vec<Option<WKB<'_, O>>> = arr.iter().collect();
        let builder = GeometryCollectionBuilder::<i32, 2>::from_wkb(&wkb_objects, Some(coord_type), arr.metadata(), true)?;
        Ok(builder.finish().downcast(true))
    }
}

impl FromWKB for ChunkedPointArray<2> {
    type Input<O: OffsetSizeTrait> = ChunkedWKBArray<O>;

    fn from_wkb<O: OffsetSizeTrait>(arr: &Self::Input<O>, coord_type: CoordType) -> Result<Self> {
        arr.try_map(|chunk| FromWKB::from_wkb(chunk, coord_type))?.try_into()
    }
}

macro_rules! impl_chunked {
    ($chunked_array:ty) => {
        impl FromWKB for $chunked_array {
            type Input<O: OffsetSizeTrait> = ChunkedWKBArray<O>;

            fn from_wkb<O: OffsetSizeTrait>(arr: &ChunkedWKBArray<O>, coord_type: CoordType) -> Result<Self> {
                arr.try_map(|chunk| FromWKB::from_wkb(chunk, coord_type))?.try_into()
            }
        }
    };
}

impl_chunked!(ChunkedLineStringArray<2>);
impl_chunked!(ChunkedPolygonArray<2>);
impl_chunked!(ChunkedMultiPointArray<2>);
impl_chunked!(ChunkedMultiLineStringArray<2>);
impl_chunked!(ChunkedMultiPolygonArray<2>);
impl_chunked!(ChunkedMixedGeometryArray<2>);
impl_chunked!(ChunkedGeometryCollectionArray<2>);

impl FromWKB for Arc<dyn ChunkedNativeArray> {
    type Input<O: OffsetSizeTrait> = ChunkedWKBArray<O>;

    fn from_wkb<O: OffsetSizeTrait>(arr: &ChunkedWKBArray<O>, coord_type: CoordType) -> Result<Self> {
        let geom_arr = ChunkedGeometryCollectionArray::<i32, 2>::from_wkb(arr, coord_type)?;
        Ok(geom_arr.downcast(true))
    }
}

/// Parse an ISO [WKBArray] to a GeometryArray with GeoArrow native encoding.
///
/// Does not downcast automatically
pub fn from_wkb<O: OffsetSizeTrait>(arr: &WKBArray<O>, target_geo_data_type: NativeType, prefer_multi: bool) -> Result<Arc<dyn NativeArray>> {
    use NativeType::*;

    let wkb_objects: Vec<Option<crate::scalar::WKB<'_, O>>> = arr.iter().collect();
    match target_geo_data_type {
        Point(coord_type, Dimension::XY) => {
            let builder = PointBuilder::<2>::from_wkb(&wkb_objects, Some(coord_type), arr.metadata())?;
            Ok(Arc::new(builder.finish()))
        }
        LineString(coord_type, Dimension::XY) => {
            let builder = LineStringBuilder::<i32, 2>::from_wkb(&wkb_objects, Some(coord_type), arr.metadata())?;
            Ok(Arc::new(builder.finish()))
        }
        Polygon(coord_type, Dimension::XY) => {
            let builder = PolygonBuilder::<i32, 2>::from_wkb(&wkb_objects, Some(coord_type), arr.metadata())?;
            Ok(Arc::new(builder.finish()))
        }
        MultiPoint(coord_type, Dimension::XY) => {
            let builder = MultiPointBuilder::<i32, 2>::from_wkb(&wkb_objects, Some(coord_type), arr.metadata())?;
            Ok(Arc::new(builder.finish()))
        }
        MultiLineString(coord_type, Dimension::XY) => {
            let builder = MultiLineStringBuilder::<i32, 2>::from_wkb(&wkb_objects, Some(coord_type), arr.metadata())?;
            Ok(Arc::new(builder.finish()))
        }
        MultiPolygon(coord_type, Dimension::XY) => {
            let builder = MultiPolygonBuilder::<i32, 2>::from_wkb(&wkb_objects, Some(coord_type), arr.metadata())?;
            Ok(Arc::new(builder.finish()))
        }
        Mixed(coord_type, Dimension::XY) => {
            let builder = MixedGeometryBuilder::<i32, 2>::from_wkb(&wkb_objects, Some(coord_type), arr.metadata(), prefer_multi)?;
            Ok(Arc::new(builder.finish()))
        }
        GeometryCollection(coord_type, Dimension::XY) => {
            let builder = GeometryCollectionBuilder::<i32, 2>::from_wkb(&wkb_objects, Some(coord_type), arr.metadata(), prefer_multi)?;
            Ok(Arc::new(builder.finish()))
        }
        Point(coord_type, Dimension::XYZ) => {
            let builder = PointBuilder::<3>::from_wkb(&wkb_objects, Some(coord_type), arr.metadata())?;
            Ok(Arc::new(builder.finish()))
        }
        LineString(coord_type, Dimension::XYZ) => {
            let builder = LineStringBuilder::<i32, 3>::from_wkb(&wkb_objects, Some(coord_type), arr.metadata())?;
            Ok(Arc::new(builder.finish()))
        }
        Polygon(coord_type, Dimension::XYZ) => {
            let builder = PolygonBuilder::<i32, 3>::from_wkb(&wkb_objects, Some(coord_type), arr.metadata())?;
            Ok(Arc::new(builder.finish()))
        }
        MultiPoint(coord_type, Dimension::XYZ) => {
            let builder = MultiPointBuilder::<i32, 3>::from_wkb(&wkb_objects, Some(coord_type), arr.metadata())?;
            Ok(Arc::new(builder.finish()))
        }
        MultiLineString(coord_type, Dimension::XYZ) => {
            let builder = MultiLineStringBuilder::<i32, 3>::from_wkb(&wkb_objects, Some(coord_type), arr.metadata())?;
            Ok(Arc::new(builder.finish()))
        }
        MultiPolygon(coord_type, Dimension::XYZ) => {
            let builder = MultiPolygonBuilder::<i32, 3>::from_wkb(&wkb_objects, Some(coord_type), arr.metadata())?;
            Ok(Arc::new(builder.finish()))
        }
        Mixed(coord_type, Dimension::XYZ) => {
            let builder = MixedGeometryBuilder::<i32, 3>::from_wkb(&wkb_objects, Some(coord_type), arr.metadata(), prefer_multi)?;
            Ok(Arc::new(builder.finish()))
        }
        GeometryCollection(coord_type, Dimension::XYZ) => {
            let builder = GeometryCollectionBuilder::<i32, 3>::from_wkb(&wkb_objects, Some(coord_type), arr.metadata(), prefer_multi)?;
            Ok(Arc::new(builder.finish()))
        }
        Rect(_) => Err(GeoArrowError::General(format!("Unexpected data type {:?}", target_geo_data_type,))),
    }
}

/// An optimized implementation of converting from ISO WKB-encoded geometries.
///
/// This implementation performs a two-pass approach, first scanning the input geometries to
/// determine the exact buffer sizes, then making a single set of allocations and filling those new
/// arrays with the WKB coordinate values.
pub trait ToWKB: Sized {
    type Output<O: OffsetSizeTrait>;

    fn to_wkb<O: OffsetSizeTrait>(&self) -> Self::Output<O>;
}

impl ToWKB for &dyn NativeArray {
    type Output<O: OffsetSizeTrait> = WKBArray<O>;

    fn to_wkb<O: OffsetSizeTrait>(&self) -> Self::Output<O> {
        use Dimension::*;
        use NativeType::*;

        match self.data_type() {
            Point(_, XY) => self.as_point::<2>().into(),
            LineString(_, XY) => self.as_line_string::<2>().into(),
            Polygon(_, XY) => self.as_polygon::<2>().into(),
            MultiPoint(_, XY) => self.as_multi_point::<2>().into(),
            MultiLineString(_, XY) => self.as_multi_line_string::<2>().into(),
            MultiPolygon(_, XY) => self.as_multi_polygon::<2>().into(),
            Mixed(_, XY) => self.as_mixed::<2>().into(),
            GeometryCollection(_, XY) => self.as_geometry_collection::<2>().into(),

            Point(_, XYZ) => self.as_point::<3>().into(),
            LineString(_, XYZ) => self.as_line_string::<3>().into(),
            Polygon(_, XYZ) => self.as_polygon::<3>().into(),
            MultiPoint(_, XYZ) => self.as_multi_point::<3>().into(),
            MultiLineString(_, XYZ) => self.as_multi_line_string::<3>().into(),
            MultiPolygon(_, XYZ) => self.as_multi_polygon::<3>().into(),
            Mixed(_, XYZ) => self.as_mixed::<3>().into(),
            GeometryCollection(_, XYZ) => self.as_geometry_collection::<3>().into(),
            Rect(_) => todo!(),
        }
    }
}

impl ToWKB for &dyn ChunkedNativeArray {
    type Output<O: OffsetSizeTrait> = ChunkedWKBArray<O>;

    fn to_wkb<O: OffsetSizeTrait>(&self) -> Self::Output<O> {
        use NativeType::*;

        match self.data_type() {
            Point(_, Dimension::XY) => ChunkedGeometryArray::new(self.as_point::<2>().map(|chunk| chunk.into())),
            LineString(_, Dimension::XY) => ChunkedGeometryArray::new(self.as_line_string::<2>().map(|chunk| chunk.into())),
            Polygon(_, Dimension::XY) => ChunkedGeometryArray::new(self.as_polygon::<2>().map(|chunk| chunk.into())),
            MultiPoint(_, Dimension::XY) => ChunkedGeometryArray::new(self.as_multi_point::<2>().map(|chunk| chunk.into())),
            MultiLineString(_, Dimension::XY) => ChunkedGeometryArray::new(self.as_multi_line_string::<2>().map(|chunk| chunk.into())),
            MultiPolygon(_, Dimension::XY) => ChunkedGeometryArray::new(self.as_multi_polygon::<2>().map(|chunk| chunk.into())),
            Mixed(_, Dimension::XY) => ChunkedGeometryArray::new(self.as_mixed::<2>().map(|chunk| chunk.into())),
            GeometryCollection(_, Dimension::XY) => ChunkedGeometryArray::new(self.as_geometry_collection::<2>().map(|chunk| chunk.into())),
            Point(_, Dimension::XYZ) => ChunkedGeometryArray::new(self.as_point::<3>().map(|chunk| chunk.into())),
            LineString(_, Dimension::XYZ) => ChunkedGeometryArray::new(self.as_line_string::<3>().map(|chunk| chunk.into())),
            Polygon(_, Dimension::XYZ) => ChunkedGeometryArray::new(self.as_polygon::<3>().map(|chunk| chunk.into())),
            MultiPoint(_, Dimension::XYZ) => ChunkedGeometryArray::new(self.as_multi_point::<3>().map(|chunk| chunk.into())),
            MultiLineString(_, Dimension::XYZ) => ChunkedGeometryArray::new(self.as_multi_line_string::<3>().map(|chunk| chunk.into())),
            MultiPolygon(_, Dimension::XYZ) => ChunkedGeometryArray::new(self.as_multi_polygon::<3>().map(|chunk| chunk.into())),
            Mixed(_, Dimension::XYZ) => ChunkedGeometryArray::new(self.as_mixed::<3>().map(|chunk| chunk.into())),
            GeometryCollection(_, Dimension::XYZ) => ChunkedGeometryArray::new(self.as_geometry_collection::<3>().map(|chunk| chunk.into())),
            Rect(_) => todo!(),
        }
    }
}

/// Convert a geometry array to a [WKBArray].
pub fn to_wkb<O: OffsetSizeTrait>(arr: &dyn NativeArray) -> WKBArray<O> {
    use NativeType::*;

    match arr.data_type() {
        Point(_, Dimension::XY) => arr.as_point::<2>().into(),
        LineString(_, Dimension::XY) => arr.as_line_string::<2>().into(),
        Polygon(_, Dimension::XY) => arr.as_polygon::<2>().into(),
        MultiPoint(_, Dimension::XY) => arr.as_multi_point::<2>().into(),
        MultiLineString(_, Dimension::XY) => arr.as_multi_line_string::<2>().into(),
        MultiPolygon(_, Dimension::XY) => arr.as_multi_polygon::<2>().into(),
        Mixed(_, Dimension::XY) => arr.as_mixed::<2>().into(),
        GeometryCollection(_, Dimension::XY) => arr.as_geometry_collection::<2>().into(),
        Point(_, Dimension::XYZ) => arr.as_point::<3>().into(),
        LineString(_, Dimension::XYZ) => arr.as_line_string::<3>().into(),
        Polygon(_, Dimension::XYZ) => arr.as_polygon::<3>().into(),
        MultiPoint(_, Dimension::XYZ) => arr.as_multi_point::<3>().into(),
        MultiLineString(_, Dimension::XYZ) => arr.as_multi_line_string::<3>().into(),
        MultiPolygon(_, Dimension::XYZ) => arr.as_multi_polygon::<3>().into(),
        Mixed(_, Dimension::XYZ) => arr.as_mixed::<3>().into(),
        GeometryCollection(_, Dimension::XYZ) => arr.as_geometry_collection::<3>().into(),
        Rect(_) => todo!(),
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test::point;

    #[test]
    fn point_round_trip_explicit_casting() {
        let arr = point::point_array();
        let wkb_arr: WKBArray<i32> = to_wkb(&arr);
        let roundtrip = from_wkb(&wkb_arr, NativeType::Point(CoordType::Interleaved, Dimension::XY), true).unwrap();
        let rt_point_arr = roundtrip.as_ref();
        let rt_point_arr_ref = rt_point_arr.as_point::<2>();
        assert_eq!(&arr, rt_point_arr_ref);
    }

    #[test]
    fn point_round_trip() {
        let arr = point::point_array();
        let wkb_arr: WKBArray<i32> = to_wkb(&arr);
        let roundtrip = from_wkb(&wkb_arr, NativeType::Mixed(CoordType::Interleaved, Dimension::XY), true).unwrap();
        let rt_ref = roundtrip.as_ref();
        let rt_mixed_arr = rt_ref.as_mixed::<2>();
        let downcasted = rt_mixed_arr.downcast(true);
        let downcasted_ref = downcasted.as_ref();
        let rt_point_arr = downcasted_ref.as_point::<2>();
        assert_eq!(&arr, rt_point_arr);
    }

    #[test]
    fn point_3d_round_trip() {
        let arr = point::point_z_array();
        let wkb_arr: WKBArray<i32> = to_wkb(&arr);
        let roundtrip_mixed = from_wkb(&wkb_arr, NativeType::Mixed(CoordType::Interleaved, Dimension::XYZ), false).unwrap();
        let rt_ref = roundtrip_mixed.as_ref();
        let rt_mixed_arr = rt_ref.as_mixed::<3>();
        assert!(rt_mixed_arr.has_points());

        let roundtrip_point = from_wkb(&wkb_arr, NativeType::Point(CoordType::Interleaved, Dimension::XYZ), false).unwrap();
        let rt_ref = roundtrip_point.as_ref();
        let rt_arr = rt_ref.as_point::<3>();
        assert_eq!(rt_arr, &arr);
    }
}
