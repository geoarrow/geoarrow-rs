use crate::array::{
    GeometryArray, LineStringArray, MultiLineStringArray, MultiPointArray, MultiPolygonArray,
    PointArray, PointBuilder, PolygonArray, WKBArray,
};
use crate::GeometryArrayTrait;
use arrow_array::OffsetSizeTrait;
use geo::BoundingRect;

/// Compute the center of geometries
///
/// This first computes the axis-aligned bounding rectangle, then takes the center of that box
pub trait Center {
    fn center(&self) -> PointArray;
}

impl Center for PointArray {
    fn center(&self) -> PointArray {
        self.clone()
    }
}

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($type:ty) => {
        impl<O: OffsetSizeTrait> Center for $type {
            fn center(&self) -> PointArray {
                let mut output_array = PointBuilder::with_capacity(self.len());
                self.iter_geo().for_each(|maybe_g| {
                    output_array.push_point(
                        maybe_g
                            .and_then(|g| g.bounding_rect().map(|rect| rect.center()))
                            .as_ref(),
                    )
                });
                output_array.into()
            }
        }
    };
}

iter_geo_impl!(LineStringArray<O>);
iter_geo_impl!(PolygonArray<O>);
iter_geo_impl!(MultiPointArray<O>);
iter_geo_impl!(MultiLineStringArray<O>);
iter_geo_impl!(MultiPolygonArray<O>);
iter_geo_impl!(WKBArray<O>);

impl<O: OffsetSizeTrait> Center for GeometryArray<O> {
    crate::geometry_array_delegate_impl! {
        fn center(&self) -> PointArray;
    }
}
