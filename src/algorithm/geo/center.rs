use crate::array::{
    GeometryArray, LineStringArray, MultiLineStringArray, MultiPointArray, MultiPolygonArray,
    MutablePointArray, PointArray, PolygonArray, WKBArray,
};
use crate::GeometryArrayTrait;
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
    ($type:ident) => {
        impl Center for $type {
            fn center(&self) -> PointArray {
                let mut output_array = MutablePointArray::with_capacity(self.len());
                self.iter_geo().for_each(|maybe_g| {
                    output_array.push_geo(
                        maybe_g.and_then(|g| g.bounding_rect().map(|rect| rect.center().into())),
                    )
                });
                output_array.into()
            }
        }
    };
}

iter_geo_impl!(LineStringArray);
iter_geo_impl!(PolygonArray);
iter_geo_impl!(MultiPointArray);
iter_geo_impl!(MultiLineStringArray);
iter_geo_impl!(MultiPolygonArray);
iter_geo_impl!(WKBArray);

impl Center for GeometryArray {
    crate::geometry_array_delegate_impl! {
        fn center(&self) -> PointArray;
    }
}
