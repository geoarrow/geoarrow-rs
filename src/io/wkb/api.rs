use std::sync::Arc;

use crate::algorithm::native::Downcast;
use crate::array::geometrycollection::GeometryCollectionBuilder;
use crate::array::mixed::array::GeometryType;
use crate::array::*;
use crate::chunked_array::*;
use crate::datatypes::GeoDataType;
use crate::error::Result;
use crate::scalar::WKB;
use crate::GeometryArrayTrait;
use arrow_array::OffsetSizeTrait;

pub trait FromWKB: Sized {
    type Input<O: OffsetSizeTrait>;

    fn from_wkb<O: OffsetSizeTrait>(arr: &Self::Input<O>, coord_type: CoordType) -> Result<Self>;
}

impl FromWKB for PointArray {
    type Input<O: OffsetSizeTrait> = WKBArray<O>;

    fn from_wkb<O: OffsetSizeTrait>(arr: &WKBArray<O>, coord_type: CoordType) -> Result<Self> {
        let wkb_objects: Vec<Option<WKB<'_, O>>> = arr.iter().collect();
        let builder = PointBuilder::from_wkb(&wkb_objects, Some(coord_type))?;
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
                let builder = <$builder>::from_wkb(&wkb_objects, Some(coord_type))?;
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
        let builder =
            MixedGeometryBuilder::<OOutput>::from_wkb(&wkb_objects, Some(coord_type), true)?;
        Ok(builder.finish())
    }
}

impl<OOutput: OffsetSizeTrait> FromWKB for GeometryCollectionArray<OOutput> {
    type Input<O: OffsetSizeTrait> = WKBArray<O>;

    fn from_wkb<O: OffsetSizeTrait>(arr: &WKBArray<O>, coord_type: CoordType) -> Result<Self> {
        let wkb_objects: Vec<Option<WKB<'_, O>>> = arr.iter().collect();
        let builder =
            GeometryCollectionBuilder::<OOutput>::from_wkb(&wkb_objects, Some(coord_type), true)?;
        Ok(builder.finish())
    }
}

impl FromWKB for Arc<dyn GeometryArrayTrait> {
    type Input<O: OffsetSizeTrait> = WKBArray<O>;

    fn from_wkb<O: OffsetSizeTrait>(arr: &WKBArray<O>, coord_type: CoordType) -> Result<Self> {
        let wkb_objects: Vec<Option<WKB<'_, O>>> = arr.iter().collect();
        let builder =
            GeometryCollectionBuilder::<i64>::from_wkb(&wkb_objects, Some(coord_type), true)?;
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

/// Parse a [WKBArray] to a GeometryArray with GeoArrow native encoding.
pub fn from_wkb<O: OffsetSizeTrait>(
    arr: &WKBArray<O>,
    large_type: bool,
    coord_type: CoordType,
    geom_type: Option<GeometryType>,
) -> Result<Arc<dyn GeometryArrayTrait>> {
    let wkb_objects: Vec<Option<WKB<'_, O>>> = arr.iter().collect();

    if let Some(geom_type) = geom_type {
        match geom_type {
            GeometryType::Point => {
                let builder = PointBuilder::from_wkb(&wkb_objects, Some(coord_type))?;
                Ok(Arc::new(builder.finish()))
            }
            GeometryType::LineString => {
                if large_type {
                    let builder =
                        LineStringBuilder::<i64>::from_wkb(&wkb_objects, Some(coord_type))?;
                    Ok(Arc::new(builder.finish()))
                } else {
                    let builder =
                        LineStringBuilder::<i32>::from_wkb(&wkb_objects, Some(coord_type))?;
                    Ok(Arc::new(builder.finish()))
                }
            }
            GeometryType::Polygon => {
                if large_type {
                    let builder = PolygonBuilder::<i64>::from_wkb(&wkb_objects, Some(coord_type))?;
                    Ok(Arc::new(builder.finish()))
                } else {
                    let builder = PolygonBuilder::<i32>::from_wkb(&wkb_objects, Some(coord_type))?;
                    Ok(Arc::new(builder.finish()))
                }
            }
            GeometryType::MultiPoint => {
                if large_type {
                    let builder =
                        MultiPointBuilder::<i64>::from_wkb(&wkb_objects, Some(coord_type))?;
                    Ok(Arc::new(builder.finish()))
                } else {
                    let builder =
                        MultiPointBuilder::<i32>::from_wkb(&wkb_objects, Some(coord_type))?;
                    Ok(Arc::new(builder.finish()))
                }
            }
            GeometryType::MultiLineString => {
                if large_type {
                    let builder =
                        MultiLineStringBuilder::<i64>::from_wkb(&wkb_objects, Some(coord_type))?;
                    Ok(Arc::new(builder.finish()))
                } else {
                    let builder =
                        MultiLineStringBuilder::<i32>::from_wkb(&wkb_objects, Some(coord_type))?;
                    Ok(Arc::new(builder.finish()))
                }
            }
            GeometryType::MultiPolygon => {
                if large_type {
                    let builder =
                        MultiPolygonBuilder::<i64>::from_wkb(&wkb_objects, Some(coord_type))?;
                    Ok(Arc::new(builder.finish()))
                } else {
                    let builder =
                        MultiPolygonBuilder::<i32>::from_wkb(&wkb_objects, Some(coord_type))?;
                    Ok(Arc::new(builder.finish()))
                }
            }
            GeometryType::GeometryCollection => {
                if large_type {
                    let builder = GeometryCollectionBuilder::<i64>::from_wkb(
                        &wkb_objects,
                        Some(coord_type),
                        true,
                    )?;
                    Ok(builder.finish().downcast(true))
                } else {
                    let builder = GeometryCollectionBuilder::<i32>::from_wkb(
                        &wkb_objects,
                        Some(coord_type),
                        true,
                    )?;
                    Ok(builder.finish().downcast(true))
                }
            }
        }
    } else {
        #[allow(clippy::collapsible_else_if)]
        if large_type {
            let builder =
                GeometryCollectionBuilder::<i64>::from_wkb(&wkb_objects, Some(coord_type), true)?;
            Ok(builder.finish().downcast(true))
        } else {
            let builder =
                GeometryCollectionBuilder::<i32>::from_wkb(&wkb_objects, Some(coord_type), true)?;
            Ok(builder.finish().downcast(true))
        }
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
        let roundtrip = from_wkb(
            &wkb_arr,
            false,
            CoordType::Interleaved,
            Some(GeometryType::Point),
        )
        .unwrap();
        let rt_point_arr = roundtrip.as_any().downcast_ref::<PointArray>().unwrap();
        assert_eq!(&arr, rt_point_arr);
    }

    #[test]
    fn point_round_trip() {
        let points = vec![point::p0(), point::p1(), point::p2()];
        let arr = PointArray::from(points.as_slice());
        let wkb_arr: WKBArray<i32> = to_wkb(&arr);
        let roundtrip = from_wkb(&wkb_arr, false, CoordType::Interleaved, None).unwrap();
        let rt_point_arr = roundtrip.as_any().downcast_ref::<PointArray>().unwrap();
        assert_eq!(&arr, rt_point_arr);
    }
}
