use crate::array::*;
use wasm_bindgen::prelude::*;

macro_rules! impl_centroid {
    ($struct_name:ident) => {
        #[wasm_bindgen]
        impl $struct_name {
            /// Calculation of the centroid.
            /// The centroid is the arithmetic mean position of all points in the shape.
            /// Informally, it is the point at which a cutout of the shape could be perfectly
            /// balanced on the tip of a pin.
            /// The geometric centroid of a convex object always lies in the object.
            /// A non-convex object might have a centroid that _is outside the object itself_.
            #[wasm_bindgen]
            pub fn centroid(&self) -> PointArray {
                use geoarrow::algorithm::geo::Centroid;
                PointArray(Centroid::centroid(&self.0))
            }
        }
    };
}

impl_centroid!(PointArray);
impl_centroid!(LineStringArray);
impl_centroid!(PolygonArray);
impl_centroid!(MultiPointArray);
impl_centroid!(MultiLineStringArray);
impl_centroid!(MultiPolygonArray);
impl_centroid!(GeometryArray);
