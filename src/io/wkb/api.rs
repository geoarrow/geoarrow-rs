use std::sync::Arc;

use crate::algorithm::native::Downcast;
use crate::array::geometrycollection::GeometryCollectionBuilder;
use crate::array::*;
use crate::chunked_array::*;
use crate::datatypes::GeoDataType;
use crate::error::{GeoArrowError, Result};
use crate::scalar::WKB;
use crate::trait_::GeometryArrayAccessor;
use crate::GeometryArrayTrait;
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

impl FromWKB for PointArray {
    type Input<O: OffsetSizeTrait> = WKBArray<O>;

    fn from_wkb<O: OffsetSizeTrait>(arr: &WKBArray<O>, coord_type: CoordType) -> Result<Self> {
        let wkb_objects: Vec<Option<WKB<'_, O>>> = arr.iter().collect();
        let builder = PointBuilder::from_wkb(&wkb_objects, Some(coord_type), arr.metadata())?;
        Ok(builder.finish())
    }
}

macro_rules! impl_from_wkb {
    ($array:ty, $builder:ty) => {
        impl<OOutput: OffsetSizeTrait> FromWKB for $array {
            type Input<O: OffsetSizeTrait> = WKBArray<O>;

            fn from_wkb<O: OffsetSizeTrait>(
                arr: &WKBArray<O>,
                coord_type: CoordType,
            ) -> Result<Self> {
                let wkb_objects: Vec<Option<WKB<'_, O>>> = arr.iter().collect();
                let builder = <$builder>::from_wkb(&wkb_objects, Some(coord_type), arr.metadata())?;
                Ok(builder.finish())
            }
        }
    };
}

impl_from_wkb!(LineStringArray<OOutput>, LineStringBuilder<OOutput>);
impl_from_wkb!(PolygonArray<OOutput>, PolygonBuilder<OOutput>);
impl_from_wkb!(MultiPointArray<OOutput>, MultiPointBuilder<OOutput>);
impl_from_wkb!(
    MultiLineStringArray<OOutput>,
    MultiLineStringBuilder<OOutput>
);
impl_from_wkb!(MultiPolygonArray<OOutput>, MultiPolygonBuilder<OOutput>);

impl<OOutput: OffsetSizeTrait> FromWKB for MixedGeometryArray<OOutput> {
    type Input<O: OffsetSizeTrait> = WKBArray<O>;

    fn from_wkb<O: OffsetSizeTrait>(arr: &WKBArray<O>, coord_type: CoordType) -> Result<Self> {
        let wkb_objects: Vec<Option<WKB<'_, O>>> = arr.iter().collect();
        let builder = MixedGeometryBuilder::<OOutput>::from_wkb(
            &wkb_objects,
            Some(coord_type),
            arr.metadata(),
            true,
        )?;
        Ok(builder.finish())
    }
}

impl<OOutput: OffsetSizeTrait> FromWKB for GeometryCollectionArray<OOutput> {
    type Input<O: OffsetSizeTrait> = WKBArray<O>;

    fn from_wkb<O: OffsetSizeTrait>(arr: &WKBArray<O>, coord_type: CoordType) -> Result<Self> {
        let wkb_objects: Vec<Option<WKB<'_, O>>> = arr.iter().collect();
        let builder = GeometryCollectionBuilder::<OOutput>::from_wkb(
            &wkb_objects,
            Some(coord_type),
            arr.metadata(),
            true,
        )?;
        Ok(builder.finish())
    }
}

impl FromWKB for Arc<dyn GeometryArrayTrait> {
    type Input<O: OffsetSizeTrait> = WKBArray<O>;

    fn from_wkb<O: OffsetSizeTrait>(arr: &WKBArray<O>, coord_type: CoordType) -> Result<Self> {
        let wkb_objects: Vec<Option<WKB<'_, O>>> = arr.iter().collect();
        let builder = GeometryCollectionBuilder::<i64>::from_wkb(
            &wkb_objects,
            Some(coord_type),
            arr.metadata(),
            true,
        )?;
        Ok(builder.finish().downcast(true))
    }
}

impl FromWKB for ChunkedPointArray {
    type Input<O: OffsetSizeTrait> = ChunkedWKBArray<O>;

    fn from_wkb<O: OffsetSizeTrait>(arr: &Self::Input<O>, coord_type: CoordType) -> Result<Self> {
        arr.try_map(|chunk| FromWKB::from_wkb(chunk, coord_type))?
            .try_into()
    }
}

macro_rules! impl_chunked {
    ($chunked_array:ty) => {
        impl<OOutput: OffsetSizeTrait> FromWKB for $chunked_array {
            type Input<O: OffsetSizeTrait> = ChunkedWKBArray<O>;

            fn from_wkb<O: OffsetSizeTrait>(
                arr: &ChunkedWKBArray<O>,
                coord_type: CoordType,
            ) -> Result<Self> {
                arr.try_map(|chunk| FromWKB::from_wkb(chunk, coord_type))?
                    .try_into()
            }
        }
    };
}

impl_chunked!(ChunkedLineStringArray<OOutput>);
impl_chunked!(ChunkedPolygonArray<OOutput>);
impl_chunked!(ChunkedMultiPointArray<OOutput>);
impl_chunked!(ChunkedMultiLineStringArray<OOutput>);
impl_chunked!(ChunkedMultiPolygonArray<OOutput>);
impl_chunked!(ChunkedMixedGeometryArray<OOutput>);
impl_chunked!(ChunkedGeometryCollectionArray<OOutput>);

/// Parse an ISO [WKBArray] to a GeometryArray with GeoArrow native encoding.
///
/// Does not downcast automatically
pub fn from_wkb<O: OffsetSizeTrait>(
    arr: &WKBArray<O>,
    target_geo_data_type: GeoDataType,
    prefer_multi: bool,
) -> Result<Arc<dyn GeometryArrayTrait>> {
    use GeoDataType::*;

    let wkb_objects: Vec<Option<crate::scalar::WKB<'_, O>>> = arr.iter().collect();
    match target_geo_data_type {
        Point(coord_type) => {
            let builder = PointBuilder::from_wkb(&wkb_objects, Some(coord_type), arr.metadata())?;
            Ok(Arc::new(builder.finish()))
        }
        LineString(coord_type) => {
            let builder =
                LineStringBuilder::<i32>::from_wkb(&wkb_objects, Some(coord_type), arr.metadata())?;
            Ok(Arc::new(builder.finish()))
        }
        LargeLineString(coord_type) => {
            let builder =
                LineStringBuilder::<i64>::from_wkb(&wkb_objects, Some(coord_type), arr.metadata())?;
            Ok(Arc::new(builder.finish()))
        }
        Polygon(coord_type) => {
            let builder =
                PolygonBuilder::<i32>::from_wkb(&wkb_objects, Some(coord_type), arr.metadata())?;
            Ok(Arc::new(builder.finish()))
        }
        LargePolygon(coord_type) => {
            let builder =
                PolygonBuilder::<i64>::from_wkb(&wkb_objects, Some(coord_type), arr.metadata())?;
            Ok(Arc::new(builder.finish()))
        }
        MultiPoint(coord_type) => {
            let builder =
                MultiPointBuilder::<i32>::from_wkb(&wkb_objects, Some(coord_type), arr.metadata())?;
            Ok(Arc::new(builder.finish()))
        }
        LargeMultiPoint(coord_type) => {
            let builder =
                MultiPointBuilder::<i64>::from_wkb(&wkb_objects, Some(coord_type), arr.metadata())?;
            Ok(Arc::new(builder.finish()))
        }
        MultiLineString(coord_type) => {
            let builder = MultiLineStringBuilder::<i32>::from_wkb(
                &wkb_objects,
                Some(coord_type),
                arr.metadata(),
            )?;
            Ok(Arc::new(builder.finish()))
        }
        LargeMultiLineString(coord_type) => {
            let builder = MultiLineStringBuilder::<i64>::from_wkb(
                &wkb_objects,
                Some(coord_type),
                arr.metadata(),
            )?;
            Ok(Arc::new(builder.finish()))
        }
        MultiPolygon(coord_type) => {
            let builder = MultiPolygonBuilder::<i32>::from_wkb(
                &wkb_objects,
                Some(coord_type),
                arr.metadata(),
            )?;
            Ok(Arc::new(builder.finish()))
        }
        LargeMultiPolygon(coord_type) => {
            let builder = MultiPolygonBuilder::<i64>::from_wkb(
                &wkb_objects,
                Some(coord_type),
                arr.metadata(),
            )?;
            Ok(Arc::new(builder.finish()))
        }
        Mixed(coord_type) => {
            let builder = MixedGeometryBuilder::<i32>::from_wkb(
                &wkb_objects,
                Some(coord_type),
                arr.metadata(),
                prefer_multi,
            )?;
            Ok(Arc::new(builder.finish()))
        }
        LargeMixed(coord_type) => {
            let builder = MixedGeometryBuilder::<i64>::from_wkb(
                &wkb_objects,
                Some(coord_type),
                arr.metadata(),
                prefer_multi,
            )?;
            Ok(Arc::new(builder.finish()))
        }
        GeometryCollection(coord_type) => {
            let builder = GeometryCollectionBuilder::<i32>::from_wkb(
                &wkb_objects,
                Some(coord_type),
                arr.metadata(),
                prefer_multi,
            )?;
            Ok(Arc::new(builder.finish()))
        }
        LargeGeometryCollection(coord_type) => {
            let builder = GeometryCollectionBuilder::<i64>::from_wkb(
                &wkb_objects,
                Some(coord_type),
                arr.metadata(),
                prefer_multi,
            )?;
            Ok(Arc::new(builder.finish()))
        }
        t => Err(GeoArrowError::General(format!(
            "Unexpected data type {:?}",
            t,
        ))),
    }
}

/// Convert a geometry array to a [WKBArray].
pub fn to_wkb<O: OffsetSizeTrait>(arr: &dyn GeometryArrayTrait) -> WKBArray<O> {
    match arr.data_type() {
        GeoDataType::Point(_) => arr.as_any().downcast_ref::<PointArray>().unwrap().into(),
        GeoDataType::LineString(_) => arr
            .as_any()
            .downcast_ref::<LineStringArray<i32>>()
            .unwrap()
            .into(),
        GeoDataType::LargeLineString(_) => arr
            .as_any()
            .downcast_ref::<LineStringArray<i64>>()
            .unwrap()
            .into(),
        GeoDataType::Polygon(_) => arr
            .as_any()
            .downcast_ref::<PolygonArray<i32>>()
            .unwrap()
            .into(),
        GeoDataType::LargePolygon(_) => arr
            .as_any()
            .downcast_ref::<PolygonArray<i64>>()
            .unwrap()
            .into(),
        GeoDataType::MultiPoint(_) => arr
            .as_any()
            .downcast_ref::<MultiPointArray<i32>>()
            .unwrap()
            .into(),
        GeoDataType::LargeMultiPoint(_) => arr
            .as_any()
            .downcast_ref::<MultiPointArray<i64>>()
            .unwrap()
            .into(),
        GeoDataType::MultiLineString(_) => arr
            .as_any()
            .downcast_ref::<MultiLineStringArray<i32>>()
            .unwrap()
            .into(),
        GeoDataType::LargeMultiLineString(_) => arr
            .as_any()
            .downcast_ref::<MultiLineStringArray<i64>>()
            .unwrap()
            .into(),
        GeoDataType::MultiPolygon(_) => arr
            .as_any()
            .downcast_ref::<MultiPolygonArray<i32>>()
            .unwrap()
            .into(),
        GeoDataType::LargeMultiPolygon(_) => arr
            .as_any()
            .downcast_ref::<MultiPolygonArray<i64>>()
            .unwrap()
            .into(),
        GeoDataType::Mixed(_) => arr
            .as_any()
            .downcast_ref::<MixedGeometryArray<i32>>()
            .unwrap()
            .into(),
        GeoDataType::LargeMixed(_) => arr
            .as_any()
            .downcast_ref::<MixedGeometryArray<i64>>()
            .unwrap()
            .into(),
        GeoDataType::GeometryCollection(_) => arr
            .as_any()
            .downcast_ref::<GeometryCollectionArray<i32>>()
            .unwrap()
            .into(),
        GeoDataType::LargeGeometryCollection(_) => arr
            .as_any()
            .downcast_ref::<GeometryCollectionArray<i64>>()
            .unwrap()
            .into(),
        GeoDataType::WKB => todo!(),
        GeoDataType::LargeWKB => todo!(),
        GeoDataType::Rect => todo!(),
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
        let roundtrip =
            from_wkb(&wkb_arr, GeoDataType::Point(CoordType::Interleaved), true).unwrap();
        let rt_point_arr = roundtrip.as_ref();
        let rt_point_arr_ref = rt_point_arr.as_point();
        assert_eq!(&arr, rt_point_arr_ref);
    }

    #[test]
    fn point_round_trip() {
        let points = vec![point::p0(), point::p1(), point::p2()];
        let arr = PointArray::from(points.as_slice());
        let wkb_arr: WKBArray<i32> = to_wkb(&arr);
        let roundtrip =
            from_wkb(&wkb_arr, GeoDataType::Mixed(CoordType::Interleaved), true).unwrap();
        let rt_ref = roundtrip.as_ref();
        let rt_mixed_arr = rt_ref.as_mixed();
        let downcasted = rt_mixed_arr.downcast(true);
        let downcasted_ref = downcasted.as_ref();
        let rt_point_arr = downcasted_ref.as_point();
        assert_eq!(&arr, rt_point_arr);
    }
}
