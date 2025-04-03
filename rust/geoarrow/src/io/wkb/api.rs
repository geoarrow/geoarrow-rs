use std::sync::Arc;

use arrow_array::OffsetSizeTrait;
use geoarrow_schema::{CoordType, Dimension};

use crate::algorithm::native::Downcast;
use crate::array::*;
use crate::chunked_array::*;
use crate::datatypes::NativeType;
use crate::error::{GeoArrowError, Result};
use crate::scalar::WKB;
use crate::trait_::ArrayAccessor;
use crate::NativeArray;

/// An optimized implementation of converting from WKB-encoded geometries.
///
/// This supports either ISO or EWKB-flavored data.
///
/// This implementation performs a two-pass approach, first scanning the input geometries to
/// determine the exact buffer sizes, then making a single set of allocations and filling those new
/// arrays with the WKB coordinate values.
pub trait FromWKB: Sized {
    /// The input array type. Either [`WKBArray`] or [`ChunkedWKBArray`]
    type Input<O: OffsetSizeTrait>;

    /// Parse the WKB input.
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
        let builder = PointBuilder::from_wkb(&wkb_objects, dim, coord_type, arr.metadata())?;
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
                let builder = <$builder>::from_wkb(&wkb_objects, dim, coord_type, arr.metadata())?;
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
        let builder =
            MixedGeometryBuilder::from_wkb(&wkb_objects, dim, coord_type, arr.metadata(), false)?;
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
            coord_type,
            arr.metadata(),
            false,
        )?;
        Ok(builder.finish())
    }
}

impl FromWKB for GeometryArray {
    type Input<O: OffsetSizeTrait> = WKBArray<O>;

    fn from_wkb<O: OffsetSizeTrait>(
        arr: &WKBArray<O>,
        coord_type: CoordType,
        _dim: Dimension,
    ) -> Result<Self> {
        let wkb_objects: Vec<Option<WKB<'_, O>>> = arr.iter().collect();
        let builder = GeometryBuilder::from_wkb(&wkb_objects, coord_type, arr.metadata(), false)?;
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
        Ok(Arc::new(GeometryArray::from_wkb(arr, coord_type, dim)?))
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
/// The returned array is guaranteed to have exactly the type of `target_type`.
///
/// `NativeType::Rect` is currently not allowed.
pub fn from_wkb<O: OffsetSizeTrait>(
    arr: &WKBArray<O>,
    target_type: NativeType,
    prefer_multi: bool,
) -> Result<Arc<dyn NativeArray>> {
    use NativeType::*;
    let wkb_objects: Vec<Option<crate::scalar::WKB<'_, O>>> = arr.iter().collect();
    match target_type {
        Point(t) => {
            let builder = PointBuilder::from_wkb(
                &wkb_objects,
                t.dimension(),
                t.coord_type(),
                arr.metadata(),
            )?;
            Ok(Arc::new(builder.finish()))
        }
        LineString(t) => {
            let builder = LineStringBuilder::from_wkb(
                &wkb_objects,
                t.dimension(),
                t.coord_type(),
                arr.metadata(),
            )?;
            Ok(Arc::new(builder.finish()))
        }
        Polygon(t) => {
            let builder = PolygonBuilder::from_wkb(
                &wkb_objects,
                t.dimension(),
                t.coord_type(),
                arr.metadata(),
            )?;
            Ok(Arc::new(builder.finish()))
        }
        MultiPoint(t) => {
            let builder = MultiPointBuilder::from_wkb(
                &wkb_objects,
                t.dimension(),
                t.coord_type(),
                arr.metadata(),
            )?;
            Ok(Arc::new(builder.finish()))
        }
        MultiLineString(t) => {
            let builder = MultiLineStringBuilder::from_wkb(
                &wkb_objects,
                t.dimension(),
                t.coord_type(),
                arr.metadata(),
            )?;
            Ok(Arc::new(builder.finish()))
        }
        MultiPolygon(t) => {
            let builder = MultiPolygonBuilder::from_wkb(
                &wkb_objects,
                t.dimension(),
                t.coord_type(),
                arr.metadata(),
            )?;
            Ok(Arc::new(builder.finish()))
        }
        GeometryCollection(t) => {
            let builder = GeometryCollectionBuilder::from_wkb(
                &wkb_objects,
                t.dimension(),
                t.coord_type(),
                arr.metadata(),
                prefer_multi,
            )?;
            Ok(Arc::new(builder.finish()))
        }
        Rect(_) => Err(GeoArrowError::General(format!(
            "Unexpected data type {:?}",
            target_type,
        ))),
        Geometry(coord_type) => {
            let builder =
                GeometryBuilder::from_wkb(&wkb_objects, coord_type, arr.metadata(), prefer_multi)?;
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
    /// The output type, either [WKBArray] or [ChunkedWKBArray]
    type Output<O: OffsetSizeTrait>;

    /// Encode as WKB
    fn to_wkb<O: OffsetSizeTrait>(&self) -> Self::Output<O>;
}

impl ToWKB for &dyn NativeArray {
    type Output<O: OffsetSizeTrait> = WKBArray<O>;

    fn to_wkb<O: OffsetSizeTrait>(&self) -> Self::Output<O> {
        use NativeType::*;

        match self.data_type() {
            Point(_) => self.as_point().into(),
            LineString(_) => self.as_line_string().into(),
            Polygon(_) => self.as_polygon().into(),
            MultiPoint(_) => self.as_multi_point().into(),
            MultiLineString(_) => self.as_multi_line_string().into(),
            MultiPolygon(_) => self.as_multi_polygon().into(),
            GeometryCollection(_) => self.as_geometry_collection().into(),
            Rect(_) => self.as_rect().into(),
            Geometry(_) => self.as_geometry().into(),
        }
    }
}

impl ToWKB for &dyn ChunkedNativeArray {
    type Output<O: OffsetSizeTrait> = ChunkedWKBArray<O>;

    fn to_wkb<O: OffsetSizeTrait>(&self) -> Self::Output<O> {
        use NativeType::*;

        match self.data_type() {
            Point(_) => ChunkedGeometryArray::new(self.as_point().map(|chunk| chunk.into())),
            LineString(_) => {
                ChunkedGeometryArray::new(self.as_line_string().map(|chunk| chunk.into()))
            }
            Polygon(_) => ChunkedGeometryArray::new(self.as_polygon().map(|chunk| chunk.into())),
            MultiPoint(_) => {
                ChunkedGeometryArray::new(self.as_multi_point().map(|chunk| chunk.into()))
            }
            MultiLineString(_) => {
                ChunkedGeometryArray::new(self.as_multi_line_string().map(|chunk| chunk.into()))
            }
            MultiPolygon(_) => {
                ChunkedGeometryArray::new(self.as_multi_polygon().map(|chunk| chunk.into()))
            }
            GeometryCollection(_) => {
                ChunkedGeometryArray::new(self.as_geometry_collection().map(|chunk| chunk.into()))
            }
            Rect(_) => todo!(),
            Geometry(_) => ChunkedGeometryArray::new(self.as_geometry().map(|chunk| chunk.into())),
        }
    }
}

/// Convert a geometry array to a [WKBArray].
pub fn to_wkb<O: OffsetSizeTrait>(arr: &dyn NativeArray) -> WKBArray<O> {
    use NativeType::*;

    match arr.data_type() {
        Point(_) => arr.as_point().into(),
        LineString(_) => arr.as_line_string().into(),
        Polygon(_) => arr.as_polygon().into(),
        MultiPoint(_) => arr.as_multi_point().into(),
        MultiLineString(_) => arr.as_multi_line_string().into(),
        MultiPolygon(_) => arr.as_multi_polygon().into(),
        GeometryCollection(_) => arr.as_geometry_collection().into(),
        Rect(_) => arr.as_rect().into(),
        Geometry(_) => arr.as_geometry().into(),
    }
}

#[cfg(test)]
mod test {
    use geoarrow_schema::{GeometryType, PointType};

    use super::*;
    use crate::test::point;

    #[test]
    fn point_round_trip_explicit_casting() {
        let arr = point::point_array();
        let wkb_arr: WKBArray<i32> = to_wkb(&arr);
        let roundtrip = from_wkb(
            &wkb_arr,
            NativeType::Point(PointType::new(
                CoordType::Interleaved,
                Dimension::XY,
                Default::default(),
            )),
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
            NativeType::Geometry(GeometryType::new(
                CoordType::Interleaved,
                Default::default(),
            )),
            true,
        )
        .unwrap();

        let rt_ref = roundtrip.as_ref();
        let rt_mixed_arr = rt_ref.as_geometry();
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
            NativeType::Geometry(GeometryType::new(
                CoordType::Interleaved,
                Default::default(),
            )),
            false,
        )
        .unwrap();
        let rt_ref = roundtrip_mixed.as_ref();
        let rt_mixed_arr = rt_ref.as_geometry();
        assert!(rt_mixed_arr.has_points(Dimension::XYZ));

        let roundtrip_point = from_wkb(
            &wkb_arr,
            NativeType::Point(PointType::new(
                CoordType::Interleaved,
                Dimension::XYZ,
                Default::default(),
            )),
            false,
        )
        .unwrap();
        let rt_ref = roundtrip_point.as_ref();
        let rt_arr = rt_ref.as_point();
        assert_eq!(rt_arr, &arr);
    }
}
