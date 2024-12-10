use std::sync::Arc;

use crate::algorithm::native::Downcast;
use crate::array::*;
use crate::chunked_array::*;
use crate::datatypes::{Dimension, NativeType};
use crate::error::{GeoArrowError, Result};
use crate::scalar::WKB;
use crate::trait_::ArrayAccessor;
use crate::NativeArray;
use arrow_array::OffsetSizeTrait;

/// An optimized implementation of converting from WKB-encoded geometries.
///
/// This supports either ISO or EWKB-flavored data.
///
/// This implementation performs a two-pass approach, first scanning the input geometries to
/// determine the exact buffer sizes, then making a single set of allocations and filling those new
/// arrays with the WKB coordinate values.
pub trait FromWKB: Sized {
    type Input<O: OffsetSizeTrait>;

    fn from_wkb<O: OffsetSizeTrait>(
        arr: &Self::Input<O>,
        coord_type: CoordType,
        dim: Dimension,
    ) -> Result<Self>;
}

impl FromWKB for PointArray {
    type Input<O: OffsetSizeTrait> = WKBArray<O>;

    fn from_wkb<O: OffsetSizeTrait>(
        arr: &WKBArray<O>,
        coord_type: CoordType,
        dim: Dimension,
    ) -> Result<Self> {
        let wkb_objects: Vec<Option<WKB<'_, O>>> = arr.iter().collect();
        let builder = PointBuilder::from_wkb(&wkb_objects, dim, Some(coord_type), arr.metadata())?;
        Ok(builder.finish())
    }
}

macro_rules! impl_from_wkb {
    ($array:ty, $builder:ty) => {
        impl FromWKB for $array {
            type Input<O: OffsetSizeTrait> = WKBArray<O>;

            fn from_wkb<O: OffsetSizeTrait>(
                arr: &WKBArray<O>,
                coord_type: CoordType,
                dim: Dimension,
            ) -> Result<Self> {
                let wkb_objects: Vec<Option<WKB<'_, O>>> = arr.iter().collect();
                let builder =
                    <$builder>::from_wkb(&wkb_objects, dim, Some(coord_type), arr.metadata())?;
                Ok(builder.finish())
            }
        }
    };
}

impl_from_wkb!(LineStringArray, LineStringBuilder);
impl_from_wkb!(PolygonArray, PolygonBuilder);
impl_from_wkb!(MultiPointArray, MultiPointBuilder);
impl_from_wkb!(MultiLineStringArray, MultiLineStringBuilder);
impl_from_wkb!(MultiPolygonArray, MultiPolygonBuilder);

impl FromWKB for MixedGeometryArray {
    type Input<O: OffsetSizeTrait> = WKBArray<O>;

    fn from_wkb<O: OffsetSizeTrait>(
        arr: &WKBArray<O>,
        coord_type: CoordType,
        dim: Dimension,
    ) -> Result<Self> {
        let wkb_objects: Vec<Option<WKB<'_, O>>> = arr.iter().collect();
        let builder = MixedGeometryBuilder::from_wkb(
            &wkb_objects,
            dim,
            Some(coord_type),
            arr.metadata(),
            true,
        )?;
        Ok(builder.finish())
    }
}

impl FromWKB for GeometryCollectionArray {
    type Input<O: OffsetSizeTrait> = WKBArray<O>;

    fn from_wkb<O: OffsetSizeTrait>(
        arr: &WKBArray<O>,
        coord_type: CoordType,
        dim: Dimension,
    ) -> Result<Self> {
        let wkb_objects: Vec<Option<WKB<'_, O>>> = arr.iter().collect();
        let builder = GeometryCollectionBuilder::from_wkb(
            &wkb_objects,
            dim,
            Some(coord_type),
            arr.metadata(),
            true,
        )?;
        Ok(builder.finish())
    }
}

impl FromWKB for Arc<dyn NativeArray> {
    type Input<O: OffsetSizeTrait> = WKBArray<O>;

    fn from_wkb<O: OffsetSizeTrait>(
        arr: &WKBArray<O>,
        coord_type: CoordType,
        dim: Dimension,
    ) -> Result<Self> {
        let wkb_objects: Vec<Option<WKB<'_, O>>> = arr.iter().collect();
        let builder = GeometryCollectionBuilder::from_wkb(
            &wkb_objects,
            dim,
            Some(coord_type),
            arr.metadata(),
            true,
        )?;
        builder.finish().downcast()
    }
}

impl FromWKB for ChunkedPointArray {
    type Input<O: OffsetSizeTrait> = ChunkedWKBArray<O>;

    fn from_wkb<O: OffsetSizeTrait>(
        arr: &Self::Input<O>,
        coord_type: CoordType,
        dim: Dimension,
    ) -> Result<Self> {
        arr.try_map(|chunk| FromWKB::from_wkb(chunk, coord_type, dim))?
            .try_into()
    }
}

macro_rules! impl_chunked {
    ($chunked_array:ty) => {
        impl FromWKB for $chunked_array {
            type Input<O: OffsetSizeTrait> = ChunkedWKBArray<O>;

            fn from_wkb<O: OffsetSizeTrait>(
                arr: &ChunkedWKBArray<O>,
                coord_type: CoordType,
                dim: Dimension,
            ) -> Result<Self> {
                arr.try_map(|chunk| FromWKB::from_wkb(chunk, coord_type, dim))?
                    .try_into()
            }
        }
    };
}

impl_chunked!(ChunkedLineStringArray);
impl_chunked!(ChunkedPolygonArray);
impl_chunked!(ChunkedMultiPointArray);
impl_chunked!(ChunkedMultiLineStringArray);
impl_chunked!(ChunkedMultiPolygonArray);
impl_chunked!(ChunkedMixedGeometryArray);
impl_chunked!(ChunkedGeometryCollectionArray);

impl FromWKB for Arc<dyn ChunkedNativeArray> {
    type Input<O: OffsetSizeTrait> = ChunkedWKBArray<O>;

    fn from_wkb<O: OffsetSizeTrait>(
        arr: &ChunkedWKBArray<O>,
        coord_type: CoordType,
        dim: Dimension,
    ) -> Result<Self> {
        let geom_arr = ChunkedGeometryCollectionArray::from_wkb(arr, coord_type, dim)?;
        Ok(geom_arr.downcast())
    }
}

/// Parse a [WKBArray] to a GeometryArray with GeoArrow native encoding.
///
/// This supports either ISO or EWKB-flavored data.
///
/// Does not downcast automatically
pub fn from_wkb<O: OffsetSizeTrait>(
    arr: &WKBArray<O>,
    target_geo_data_type: NativeType,
    prefer_multi: bool,
) -> Result<Arc<dyn NativeArray>> {
    use NativeType::*;
    let wkb_objects: Vec<Option<crate::scalar::WKB<'_, O>>> = arr.iter().collect();
    match target_geo_data_type {
        Point(coord_type, dim) => {
            let builder =
                PointBuilder::from_wkb(&wkb_objects, dim, Some(coord_type), arr.metadata())?;
            Ok(Arc::new(builder.finish()))
        }
        LineString(coord_type, dim) => {
            let builder =
                LineStringBuilder::from_wkb(&wkb_objects, dim, Some(coord_type), arr.metadata())?;
            Ok(Arc::new(builder.finish()))
        }
        Polygon(coord_type, dim) => {
            let builder =
                PolygonBuilder::from_wkb(&wkb_objects, dim, Some(coord_type), arr.metadata())?;
            Ok(Arc::new(builder.finish()))
        }
        MultiPoint(coord_type, dim) => {
            let builder =
                MultiPointBuilder::from_wkb(&wkb_objects, dim, Some(coord_type), arr.metadata())?;
            Ok(Arc::new(builder.finish()))
        }
        MultiLineString(coord_type, dim) => {
            let builder = MultiLineStringBuilder::from_wkb(
                &wkb_objects,
                dim,
                Some(coord_type),
                arr.metadata(),
            )?;
            Ok(Arc::new(builder.finish()))
        }
        MultiPolygon(coord_type, dim) => {
            let builder =
                MultiPolygonBuilder::from_wkb(&wkb_objects, dim, Some(coord_type), arr.metadata())?;
            Ok(Arc::new(builder.finish()))
        }
        Mixed(coord_type, dim) => {
            let builder = MixedGeometryBuilder::from_wkb(
                &wkb_objects,
                dim,
                Some(coord_type),
                arr.metadata(),
                prefer_multi,
            )?;
            Ok(Arc::new(builder.finish()))
        }
        GeometryCollection(coord_type, dim) => {
            let builder = GeometryCollectionBuilder::from_wkb(
                &wkb_objects,
                dim,
                Some(coord_type),
                arr.metadata(),
                prefer_multi,
            )?;
            Ok(Arc::new(builder.finish()))
        }
        Rect(_) => Err(GeoArrowError::General(format!(
            "Unexpected data type {:?}",
            target_geo_data_type,
        ))),
        Geometry(coord_type) => {
            let builder = GeometryBuilder::from_wkb(
                &wkb_objects,
                Some(coord_type),
                arr.metadata(),
                prefer_multi,
            )?;
            Ok(Arc::new(builder.finish()))
        }
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
        use NativeType::*;

        match self.data_type() {
            Point(_, _) => self.as_point().into(),
            LineString(_, _) => self.as_line_string().into(),
            Polygon(_, _) => self.as_polygon().into(),
            MultiPoint(_, _) => self.as_multi_point().into(),
            MultiLineString(_, _) => self.as_multi_line_string().into(),
            MultiPolygon(_, _) => self.as_multi_polygon().into(),
            Mixed(_, _) => self.as_mixed().into(),
            GeometryCollection(_, _) => self.as_geometry_collection().into(),

            Rect(_) => todo!(),
            Geometry(_) => self.as_geometry().into(),
        }
    }
}

impl ToWKB for &dyn ChunkedNativeArray {
    type Output<O: OffsetSizeTrait> = ChunkedWKBArray<O>;

    fn to_wkb<O: OffsetSizeTrait>(&self) -> Self::Output<O> {
        use NativeType::*;

        match self.data_type() {
            Point(_, _) => ChunkedGeometryArray::new(self.as_point().map(|chunk| chunk.into())),
            LineString(_, _) => {
                ChunkedGeometryArray::new(self.as_line_string().map(|chunk| chunk.into()))
            }
            Polygon(_, _) => ChunkedGeometryArray::new(self.as_polygon().map(|chunk| chunk.into())),
            MultiPoint(_, _) => {
                ChunkedGeometryArray::new(self.as_multi_point().map(|chunk| chunk.into()))
            }
            MultiLineString(_, _) => {
                ChunkedGeometryArray::new(self.as_multi_line_string().map(|chunk| chunk.into()))
            }
            MultiPolygon(_, _) => {
                ChunkedGeometryArray::new(self.as_multi_polygon().map(|chunk| chunk.into()))
            }
            Mixed(_, _) => ChunkedGeometryArray::new(self.as_mixed().map(|chunk| chunk.into())),
            GeometryCollection(_, _) => {
                ChunkedGeometryArray::new(self.as_geometry_collection().map(|chunk| chunk.into()))
            }
            Rect(_) => todo!(),
            Geometry(_) => ChunkedGeometryArray::new(self.as_mixed().map(|chunk| chunk.into())),
        }
    }
}

/// Convert a geometry array to a [WKBArray].
pub fn to_wkb<O: OffsetSizeTrait>(arr: &dyn NativeArray) -> WKBArray<O> {
    use NativeType::*;

    match arr.data_type() {
        Point(_, _) => arr.as_point().into(),
        LineString(_, _) => arr.as_line_string().into(),
        Polygon(_, _) => arr.as_polygon().into(),
        MultiPoint(_, _) => arr.as_multi_point().into(),
        MultiLineString(_, _) => arr.as_multi_line_string().into(),
        MultiPolygon(_, _) => arr.as_multi_polygon().into(),
        Mixed(_, _) => arr.as_mixed().into(),
        GeometryCollection(_, _) => arr.as_geometry_collection().into(),
        Rect(_) => todo!(),
        Geometry(_) => arr.as_geometry().into(),
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
            NativeType::Point(CoordType::Interleaved, Dimension::XY),
            true,
        )
        .unwrap();
        let rt_point_arr = roundtrip.as_ref();
        let rt_point_arr_ref = rt_point_arr.as_point();
        assert_eq!(&arr, rt_point_arr_ref);
    }

    #[test]
    fn point_round_trip() {
        let arr = point::point_array();
        let wkb_arr: WKBArray<i32> = to_wkb(&arr);
        let roundtrip = from_wkb(
            &wkb_arr,
            NativeType::Mixed(CoordType::Interleaved, Dimension::XY),
            true,
        )
        .unwrap();
        let rt_ref = roundtrip.as_ref();
        let rt_mixed_arr = rt_ref.as_mixed();
        let downcasted = rt_mixed_arr.downcast().unwrap();
        let downcasted_ref = downcasted.as_ref();
        let rt_point_arr = downcasted_ref.as_point();
        assert_eq!(&arr, rt_point_arr);
    }

    #[test]
    fn point_3d_round_trip() {
        let arr = point::point_z_array();
        let wkb_arr: WKBArray<i32> = to_wkb(&arr);
        let roundtrip_mixed = from_wkb(
            &wkb_arr,
            NativeType::Mixed(CoordType::Interleaved, Dimension::XYZ),
            false,
        )
        .unwrap();
        let rt_ref = roundtrip_mixed.as_ref();
        let rt_mixed_arr = rt_ref.as_mixed();
        assert!(rt_mixed_arr.has_points());

        let roundtrip_point = from_wkb(
            &wkb_arr,
            NativeType::Point(CoordType::Interleaved, Dimension::XYZ),
            false,
        )
        .unwrap();
        let rt_ref = roundtrip_point.as_ref();
        let rt_arr = rt_ref.as_point();
        assert_eq!(rt_arr, &arr);
    }
}
