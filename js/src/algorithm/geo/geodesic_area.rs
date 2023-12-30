use crate::data::*;
use crate::vector::*;
use arrow_wasm::arrow1::data::Float64Data;
use arrow_wasm::arrow1::vector::Float64Vector;
use wasm_bindgen::prelude::*;

macro_rules! impl_geodesic_area {
    ($struct_name:ident) => {
        #[wasm_bindgen]
        impl $struct_name {
            /// Determine the area of a geometry on an ellipsoidal model of the earth.
            ///
            /// This uses the geodesic measurement methods given by [Karney (2013)].
            ///
            /// ## Assumptions
            ///  - Polygons are assumed to be wound in a counter-clockwise direction
            ///    for the exterior ring and a clockwise direction for interior rings.
            ///    This is the standard winding for geometries that follow the Simple Feature standard.
            ///    Alternative windings may result in a negative area. See "Interpreting negative area values" below.
            ///  - Polygons are assumed to be smaller than half the size of the earth. If you expect to be dealing
            ///    with polygons larger than this, please use the `unsigned` methods.
            ///
            /// ## Units
            ///
            /// - return value: meter²
            ///
            /// ## Interpreting negative area values
            ///
            /// A negative value can mean one of two things:
            /// 1. The winding of the polygon is in the clockwise direction (reverse winding). If this is the case, and you know the polygon is smaller than half the area of earth, you can take the absolute value of the reported area to get the correct area.
            /// 2. The polygon is larger than half the planet. In this case, the returned area of the polygon is not correct. If you expect to be dealing with very large polygons, please use the `unsigned` methods.
            ///
            /// [Karney (2013)]:  https://arxiv.org/pdf/1109.4448.pdf
            #[wasm_bindgen(js_name = geodesicAreaSigned)]
            pub fn geodesic_area_signed(&self) -> Float64Data {
                use geoarrow::algorithm::geo::GeodesicArea;
                GeodesicArea::geodesic_area_signed(&self.0).into()
            }

            /// Determine the area of a geometry on an ellipsoidal model of the earth. Supports very large geometries that cover a significant portion of the earth.
            ///
            /// This uses the geodesic measurement methods given by [Karney (2013)].
            ///
            /// ## Assumptions
            ///  - Polygons are assumed to be wound in a counter-clockwise direction
            ///    for the exterior ring and a clockwise direction for interior rings.
            ///    This is the standard winding for geometries that follow the Simple Features standard.
            ///    Using alternative windings will result in incorrect results.
            ///
            /// ## Units
            ///
            /// - return value: meter²
            ///
            /// [Karney (2013)]:  https://arxiv.org/pdf/1109.4448.pdf
            #[wasm_bindgen(js_name = geodesicAreaUnsigned)]
            pub fn geodesic_area_unsigned(&self) -> Float64Data {
                use geoarrow::algorithm::geo::GeodesicArea;
                GeodesicArea::geodesic_area_unsigned(&self.0).into()
            }

            /// Determine the perimeter of a geometry on an ellipsoidal model of the earth.
            ///
            /// This uses the geodesic measurement methods given by [Karney (2013)].
            ///
            /// For a polygon this returns the sum of the perimeter of the exterior ring and interior rings.
            /// To get the perimeter of just the exterior ring of a polygon, do `polygon.exterior().geodesic_length()`.
            ///
            /// ## Units
            ///
            /// - return value: meter
            ///
            /// [Karney (2013)]:  https://arxiv.org/pdf/1109.4448.pdf
            #[wasm_bindgen(js_name = geodesicPerimeter)]
            pub fn geodesic_perimeter(&self) -> Float64Data {
                use geoarrow::algorithm::geo::GeodesicArea;
                GeodesicArea::geodesic_perimeter(&self.0).into()
            }
        }
    };
}

impl_geodesic_area!(PointData);
impl_geodesic_area!(LineStringData);
impl_geodesic_area!(PolygonData);
impl_geodesic_area!(MultiPointData);
impl_geodesic_area!(MultiLineStringData);
impl_geodesic_area!(MultiPolygonData);
impl_geodesic_area!(MixedGeometryData);
impl_geodesic_area!(GeometryCollectionData);

macro_rules! impl_vector {
    ($struct_name:ident) => {
        #[wasm_bindgen]
        impl $struct_name {
            /// Determine the area of a geometry on an ellipsoidal model of the earth.
            ///
            /// This uses the geodesic measurement methods given by [Karney (2013)].
            ///
            /// ## Assumptions
            ///  - Polygons are assumed to be wound in a counter-clockwise direction
            ///    for the exterior ring and a clockwise direction for interior rings.
            ///    This is the standard winding for geometries that follow the Simple Feature standard.
            ///    Alternative windings may result in a negative area. See "Interpreting negative area values" below.
            ///  - Polygons are assumed to be smaller than half the size of the earth. If you expect to be dealing
            ///    with polygons larger than this, please use the `unsigned` methods.
            ///
            /// ## Units
            ///
            /// - return value: meter²
            ///
            /// ## Interpreting negative area values
            ///
            /// A negative value can mean one of two things:
            /// 1. The winding of the polygon is in the clockwise direction (reverse winding). If this is the case, and you know the polygon is smaller than half the area of earth, you can take the absolute value of the reported area to get the correct area.
            /// 2. The polygon is larger than half the planet. In this case, the returned area of the polygon is not correct. If you expect to be dealing with very large polygons, please use the `unsigned` methods.
            ///
            /// [Karney (2013)]:  https://arxiv.org/pdf/1109.4448.pdf
            #[wasm_bindgen(js_name = geodesicAreaSigned)]
            pub fn geodesic_area_signed(&self) -> Float64Vector {
                use geoarrow::algorithm::geo::GeodesicArea;
                Float64Vector::new(
                    GeodesicArea::geodesic_area_signed(&self.0)
                        .unwrap()
                        .into_inner(),
                )
            }

            /// Determine the area of a geometry on an ellipsoidal model of the earth. Supports very large geometries that cover a significant portion of the earth.
            ///
            /// This uses the geodesic measurement methods given by [Karney (2013)].
            ///
            /// ## Assumptions
            ///  - Polygons are assumed to be wound in a counter-clockwise direction
            ///    for the exterior ring and a clockwise direction for interior rings.
            ///    This is the standard winding for geometries that follow the Simple Features standard.
            ///    Using alternative windings will result in incorrect results.
            ///
            /// ## Units
            ///
            /// - return value: meter²
            ///
            /// [Karney (2013)]:  https://arxiv.org/pdf/1109.4448.pdf
            #[wasm_bindgen(js_name = geodesicAreaUnsigned)]
            pub fn geodesic_area_unsigned(&self) -> Float64Vector {
                use geoarrow::algorithm::geo::GeodesicArea;
                Float64Vector::new(
                    GeodesicArea::geodesic_area_unsigned(&self.0)
                        .unwrap()
                        .into_inner(),
                )
            }

            /// Determine the perimeter of a geometry on an ellipsoidal model of the earth.
            ///
            /// This uses the geodesic measurement methods given by [Karney (2013)].
            ///
            /// For a polygon this returns the sum of the perimeter of the exterior ring and interior rings.
            /// To get the perimeter of just the exterior ring of a polygon, do `polygon.exterior().geodesic_length()`.
            ///
            /// ## Units
            ///
            /// - return value: meter
            ///
            /// [Karney (2013)]:  https://arxiv.org/pdf/1109.4448.pdf
            #[wasm_bindgen(js_name = geodesicPerimeter)]
            pub fn geodesic_perimeter(&self) -> Float64Vector {
                use geoarrow::algorithm::geo::GeodesicArea;
                Float64Vector::new(
                    GeodesicArea::geodesic_perimeter(&self.0)
                        .unwrap()
                        .into_inner(),
                )
            }
        }
    };
}

impl_vector!(PointVector);
impl_vector!(LineStringVector);
impl_vector!(PolygonVector);
impl_vector!(MultiPointVector);
impl_vector!(MultiLineStringVector);
impl_vector!(MultiPolygonVector);
impl_vector!(MixedGeometryVector);
impl_vector!(GeometryCollectionVector);
