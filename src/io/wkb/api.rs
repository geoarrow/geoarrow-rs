use std::sync::Arc;

use crate::algorithm::native::Downcast;
use crate::array::geometrycollection::GeometryCollectionBuilder;
use crate::array::*;
use crate::chunked_array::*;
use crate::datatypes::{Dimension, GeoDataType};
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

impl_from_wkb!(LineStringArray<OOutput, 2>, LineStringBuilder<OOutput, 2>);
impl_from_wkb!(PolygonArray<OOutput, 2>, PolygonBuilder<OOutput, 2>);
impl_from_wkb!(MultiPointArray<OOutput, 2>, MultiPointBuilder<OOutput, 2>);
impl_from_wkb!(
    MultiLineStringArray<OOutput, 2>,
    MultiLineStringBuilder<OOutput, 2>
);
impl_from_wkb!(MultiPolygonArray<OOutput, 2>, MultiPolygonBuilder<OOutput, 2>);

impl<OOutput: OffsetSizeTrait> FromWKB for MixedGeometryArray<OOutput, 2> {
    type Input<O: OffsetSizeTrait> = WKBArray<O>;

    fn from_wkb<O: OffsetSizeTrait>(arr: &WKBArray<O>, coord_type: CoordType) -> Result<Self> {
        let wkb_objects: Vec<Option<WKB<'_, O>>> = arr.iter().collect();
        let builder = MixedGeometryBuilder::<OOutput, 2>::from_wkb(
            &wkb_objects,
            Some(coord_type),
            arr.metadata(),
            true,
        )?;
        Ok(builder.finish())
    }
}

impl<OOutput: OffsetSizeTrait> FromWKB for GeometryCollectionArray<OOutput, 2> {
    type Input<O: OffsetSizeTrait> = WKBArray<O>;

    fn from_wkb<O: OffsetSizeTrait>(arr: &WKBArray<O>, coord_type: CoordType) -> Result<Self> {
        let wkb_objects: Vec<Option<WKB<'_, O>>> = arr.iter().collect();
        let builder = GeometryCollectionBuilder::<OOutput, 2>::from_wkb(
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
        let builder = GeometryCollectionBuilder::<i64, 2>::from_wkb(
            &wkb_objects,
            Some(coord_type),
            arr.metadata(),
            true,
        )?;
        Ok(builder.finish().downcast(true))
    }
}

impl FromWKB for ChunkedPointArray<2> {
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

impl_chunked!(ChunkedLineStringArray<OOutput, 2>);
impl_chunked!(ChunkedPolygonArray<OOutput, 2>);
impl_chunked!(ChunkedMultiPointArray<OOutput, 2>);
impl_chunked!(ChunkedMultiLineStringArray<OOutput, 2>);
impl_chunked!(ChunkedMultiPolygonArray<OOutput, 2>);
impl_chunked!(ChunkedMixedGeometryArray<OOutput, 2>);
impl_chunked!(ChunkedGeometryCollectionArray<OOutput, 2>);

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
        Point(coord_type, Dimension::XY) => {
            let builder =
                PointBuilder::<2>::from_wkb(&wkb_objects, Some(coord_type), arr.metadata())?;
            Ok(Arc::new(builder.finish()))
        }
        LineString(coord_type, Dimension::XY) => {
            let builder = LineStringBuilder::<i32, 2>::from_wkb(
                &wkb_objects,
                Some(coord_type),
                arr.metadata(),
            )?;
            Ok(Arc::new(builder.finish()))
        }
        LargeLineString(coord_type, Dimension::XY) => {
            let builder = LineStringBuilder::<i64, 2>::from_wkb(
                &wkb_objects,
                Some(coord_type),
                arr.metadata(),
            )?;
            Ok(Arc::new(builder.finish()))
        }
        Polygon(coord_type, Dimension::XY) => {
            let builder =
                PolygonBuilder::<i32, 2>::from_wkb(&wkb_objects, Some(coord_type), arr.metadata())?;
            Ok(Arc::new(builder.finish()))
        }
        LargePolygon(coord_type, Dimension::XY) => {
            let builder =
                PolygonBuilder::<i64, 2>::from_wkb(&wkb_objects, Some(coord_type), arr.metadata())?;
            Ok(Arc::new(builder.finish()))
        }
        MultiPoint(coord_type, Dimension::XY) => {
            let builder = MultiPointBuilder::<i32, 2>::from_wkb(
                &wkb_objects,
                Some(coord_type),
                arr.metadata(),
            )?;
            Ok(Arc::new(builder.finish()))
        }
        LargeMultiPoint(coord_type, Dimension::XY) => {
            let builder = MultiPointBuilder::<i64, 2>::from_wkb(
                &wkb_objects,
                Some(coord_type),
                arr.metadata(),
            )?;
            Ok(Arc::new(builder.finish()))
        }
        MultiLineString(coord_type, Dimension::XY) => {
            let builder = MultiLineStringBuilder::<i32, 2>::from_wkb(
                &wkb_objects,
                Some(coord_type),
                arr.metadata(),
            )?;
            Ok(Arc::new(builder.finish()))
        }
        LargeMultiLineString(coord_type, Dimension::XY) => {
            let builder = MultiLineStringBuilder::<i64, 2>::from_wkb(
                &wkb_objects,
                Some(coord_type),
                arr.metadata(),
            )?;
            Ok(Arc::new(builder.finish()))
        }
        MultiPolygon(coord_type, Dimension::XY) => {
            let builder = MultiPolygonBuilder::<i32, 2>::from_wkb(
                &wkb_objects,
                Some(coord_type),
                arr.metadata(),
            )?;
            Ok(Arc::new(builder.finish()))
        }
        LargeMultiPolygon(coord_type, Dimension::XY) => {
            let builder = MultiPolygonBuilder::<i64, 2>::from_wkb(
                &wkb_objects,
                Some(coord_type),
                arr.metadata(),
            )?;
            Ok(Arc::new(builder.finish()))
        }
        Mixed(coord_type, Dimension::XY) => {
            let builder = MixedGeometryBuilder::<i32, 2>::from_wkb(
                &wkb_objects,
                Some(coord_type),
                arr.metadata(),
                prefer_multi,
            )?;
            Ok(Arc::new(builder.finish()))
        }
        LargeMixed(coord_type, Dimension::XY) => {
            let builder = MixedGeometryBuilder::<i64, 2>::from_wkb(
                &wkb_objects,
                Some(coord_type),
                arr.metadata(),
                prefer_multi,
            )?;
            Ok(Arc::new(builder.finish()))
        }
        GeometryCollection(coord_type, Dimension::XY) => {
            let builder = GeometryCollectionBuilder::<i32, 2>::from_wkb(
                &wkb_objects,
                Some(coord_type),
                arr.metadata(),
                prefer_multi,
            )?;
            Ok(Arc::new(builder.finish()))
        }
        LargeGeometryCollection(coord_type, Dimension::XY) => {
            let builder = GeometryCollectionBuilder::<i64, 2>::from_wkb(
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

/// An optimized implementation of converting from ISO WKB-encoded geometries.
///
/// This implementation performs a two-pass approach, first scanning the input geometries to
/// determine the exact buffer sizes, then making a single set of allocations and filling those new
/// arrays with the WKB coordinate values.
pub trait ToWKB: Sized {
    type Output<O: OffsetSizeTrait>;

    fn to_wkb<O: OffsetSizeTrait>(&self) -> Self::Output<O>;
}

impl ToWKB for &dyn GeometryArrayTrait {
    type Output<O: OffsetSizeTrait> = WKBArray<O>;

    fn to_wkb<O: OffsetSizeTrait>(&self) -> Self::Output<O> {
        match self.data_type() {
            GeoDataType::Point(_, Dimension::XY) => self.as_point_2d().into(),
            GeoDataType::LineString(_, Dimension::XY) => self.as_line_string_2d().into(),
            GeoDataType::LargeLineString(_, Dimension::XY) => self.as_large_line_string_2d().into(),
            GeoDataType::Polygon(_, Dimension::XY) => self.as_polygon_2d().into(),
            GeoDataType::LargePolygon(_, Dimension::XY) => self.as_large_polygon_2d().into(),
            GeoDataType::MultiPoint(_, Dimension::XY) => self.as_multi_point_2d().into(),
            GeoDataType::LargeMultiPoint(_, Dimension::XY) => self.as_large_multi_point_2d().into(),
            GeoDataType::MultiLineString(_, Dimension::XY) => self.as_multi_line_string_2d().into(),
            GeoDataType::LargeMultiLineString(_, Dimension::XY) => {
                self.as_large_multi_line_string_2d().into()
            }
            GeoDataType::MultiPolygon(_, Dimension::XY) => self.as_multi_polygon_2d().into(),
            GeoDataType::LargeMultiPolygon(_, Dimension::XY) => {
                self.as_large_multi_polygon_2d().into()
            }
            GeoDataType::Mixed(_, Dimension::XY) => self.as_mixed_2d().into(),
            GeoDataType::LargeMixed(_, Dimension::XY) => self.as_large_mixed_2d().into(),
            GeoDataType::GeometryCollection(_, Dimension::XY) => {
                self.as_geometry_collection_2d().into()
            }
            GeoDataType::LargeGeometryCollection(_, Dimension::XY) => {
                self.as_large_geometry_collection_2d().into()
            }
            GeoDataType::WKB => todo!(),
            GeoDataType::LargeWKB => todo!(),
            GeoDataType::Rect(_) => todo!(),
            _ => todo!("3d types"),
        }
    }
}

impl ToWKB for &dyn ChunkedGeometryArrayTrait {
    type Output<O: OffsetSizeTrait> = ChunkedWKBArray<O>;

    fn to_wkb<O: OffsetSizeTrait>(&self) -> Self::Output<O> {
        match self.data_type() {
            GeoDataType::Point(_, Dimension::XY) => {
                ChunkedGeometryArray::new(self.as_point_2d().map(|chunk| chunk.into()))
            }
            GeoDataType::LineString(_, Dimension::XY) => {
                ChunkedGeometryArray::new(self.as_line_string_2d().map(|chunk| chunk.into()))
            }
            GeoDataType::LargeLineString(_, Dimension::XY) => {
                ChunkedGeometryArray::new(self.as_large_line_string_2d().map(|chunk| chunk.into()))
            }
            GeoDataType::Polygon(_, Dimension::XY) => {
                ChunkedGeometryArray::new(self.as_polygon_2d().map(|chunk| chunk.into()))
            }
            GeoDataType::LargePolygon(_, Dimension::XY) => {
                ChunkedGeometryArray::new(self.as_large_polygon_2d().map(|chunk| chunk.into()))
            }
            GeoDataType::MultiPoint(_, Dimension::XY) => {
                ChunkedGeometryArray::new(self.as_multi_point_2d().map(|chunk| chunk.into()))
            }
            GeoDataType::LargeMultiPoint(_, Dimension::XY) => {
                ChunkedGeometryArray::new(self.as_large_multi_point_2d().map(|chunk| chunk.into()))
            }
            GeoDataType::MultiLineString(_, Dimension::XY) => {
                ChunkedGeometryArray::new(self.as_multi_line_string_2d().map(|chunk| chunk.into()))
            }
            GeoDataType::LargeMultiLineString(_, Dimension::XY) => ChunkedGeometryArray::new(
                self.as_large_multi_line_string_2d()
                    .map(|chunk| chunk.into()),
            ),
            GeoDataType::MultiPolygon(_, Dimension::XY) => {
                ChunkedGeometryArray::new(self.as_multi_polygon_2d().map(|chunk| chunk.into()))
            }
            GeoDataType::LargeMultiPolygon(_, Dimension::XY) => ChunkedGeometryArray::new(
                self.as_large_multi_polygon_2d().map(|chunk| chunk.into()),
            ),
            GeoDataType::Mixed(_, Dimension::XY) => {
                ChunkedGeometryArray::new(self.as_mixed_2d().map(|chunk| chunk.into()))
            }
            GeoDataType::LargeMixed(_, Dimension::XY) => {
                ChunkedGeometryArray::new(self.as_large_mixed_2d().map(|chunk| chunk.into()))
            }
            GeoDataType::GeometryCollection(_, Dimension::XY) => ChunkedGeometryArray::new(
                self.as_geometry_collection_2d().map(|chunk| chunk.into()),
            ),
            GeoDataType::LargeGeometryCollection(_, Dimension::XY) => ChunkedGeometryArray::new(
                self.as_large_geometry_collection_2d()
                    .map(|chunk| chunk.into()),
            ),
            GeoDataType::WKB => todo!(),
            GeoDataType::LargeWKB => todo!(),
            GeoDataType::Rect(_) => todo!(),
            _ => todo!("3d types"),
        }
    }
}

/// Convert a geometry array to a [WKBArray].
pub fn to_wkb<O: OffsetSizeTrait>(arr: &dyn GeometryArrayTrait) -> WKBArray<O> {
    match arr.data_type() {
        GeoDataType::Point(_, Dimension::XY) => arr.as_point_2d().into(),
        GeoDataType::LineString(_, Dimension::XY) => arr.as_line_string_2d().into(),
        GeoDataType::LargeLineString(_, Dimension::XY) => arr.as_large_line_string_2d().into(),
        GeoDataType::Polygon(_, Dimension::XY) => arr.as_polygon_2d().into(),
        GeoDataType::LargePolygon(_, Dimension::XY) => arr.as_large_polygon_2d().into(),
        GeoDataType::MultiPoint(_, Dimension::XY) => arr.as_multi_point_2d().into(),
        GeoDataType::LargeMultiPoint(_, Dimension::XY) => arr.as_large_multi_point_2d().into(),
        GeoDataType::MultiLineString(_, Dimension::XY) => arr.as_multi_line_string_2d().into(),
        GeoDataType::LargeMultiLineString(_, Dimension::XY) => {
            arr.as_large_multi_line_string_2d().into()
        }
        GeoDataType::MultiPolygon(_, Dimension::XY) => arr.as_multi_polygon_2d().into(),
        GeoDataType::LargeMultiPolygon(_, Dimension::XY) => arr.as_large_multi_polygon_2d().into(),
        GeoDataType::Mixed(_, Dimension::XY) => arr.as_mixed_2d().into(),
        GeoDataType::LargeMixed(_, Dimension::XY) => arr.as_large_mixed_2d().into(),
        GeoDataType::GeometryCollection(_, Dimension::XY) => arr.as_geometry_collection_2d().into(),
        GeoDataType::LargeGeometryCollection(_, Dimension::XY) => {
            arr.as_large_geometry_collection_2d().into()
        }
        GeoDataType::WKB => todo!(),
        GeoDataType::LargeWKB => todo!(),
        GeoDataType::Rect(_) => todo!(),
        _ => todo!("3d types"),
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
        let roundtrip = from_wkb(
            &wkb_arr,
            GeoDataType::Point(CoordType::Interleaved, Dimension::XY),
            true,
        )
        .unwrap();
        let rt_point_arr = roundtrip.as_ref();
        let rt_point_arr_ref = rt_point_arr.as_point_2d();
        assert_eq!(&arr, rt_point_arr_ref);
    }

    #[test]
    fn point_round_trip() {
        let points = vec![point::p0(), point::p1(), point::p2()];
        let arr = PointArray::from(points.as_slice());
        let wkb_arr: WKBArray<i32> = to_wkb(&arr);
        let roundtrip = from_wkb(
            &wkb_arr,
            GeoDataType::Mixed(CoordType::Interleaved, Dimension::XY),
            true,
        )
        .unwrap();
        let rt_ref = roundtrip.as_ref();
        let rt_mixed_arr = rt_ref.as_mixed_2d();
        let downcasted = rt_mixed_arr.downcast(true);
        let downcasted_ref = downcasted.as_ref();
        let rt_point_arr = downcasted_ref.as_point_2d();
        assert_eq!(&arr, rt_point_arr);
    }
}
