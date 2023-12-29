use crate::data::*;
use geoarrow::trait_::GeometryArraySelfMethods;
use geoarrow::GeometryArrayTrait;
use wasm_bindgen::prelude::*;

/// GeoArrow permits coordinate types to either be `Interleaved`, where the X and Y coordinates are
/// in a single buffer as XYXYXY or `Separated`, where the X and Y coordinates are in multiple
/// buffers as XXXX and YYYY.
#[wasm_bindgen]
pub enum CoordType {
    /// Coordinates are stored in a single buffer as XYXYXY
    Interleaved,

    /// Coordinates are stored in multiple buffers as XXXX, YYYY
    Separated,
}

impl From<CoordType> for geoarrow::array::CoordType {
    fn from(value: CoordType) -> Self {
        match value {
            CoordType::Interleaved => geoarrow::array::CoordType::Interleaved,
            CoordType::Separated => geoarrow::array::CoordType::Separated,
        }
    }
}

impl From<geoarrow::array::CoordType> for CoordType {
    fn from(value: geoarrow::array::CoordType) -> Self {
        match value {
            geoarrow::array::CoordType::Interleaved => CoordType::Interleaved,
            geoarrow::array::CoordType::Separated => CoordType::Separated,
        }
    }
}

macro_rules! impl_coord_type {
    ($struct_name:ident) => {
        #[wasm_bindgen]
        impl $struct_name {
            /// Get the coordinate type of this array.
            ///
            /// GeoArrow permits coordinate types to either be `Interleaved`, where the X and Y
            /// coordinates are in a single buffer as XYXYXY or `Separated`, where the X and Y
            /// coordinates are in multiple buffers as XXXX and YYYY.
            ///
            /// The Rust GeoArrow implementation supports both, so this function will tell you
            /// which coordinate type is currently used by this array. You can use
            /// `intoCoordType` to convert to either of the two coordinate layouts.
            #[wasm_bindgen(js_name = coordType)]
            pub fn coord_type(&self) -> CoordType {
                self.0.coord_type().into()
            }

            /// Convert this geometry array into another coordinate type.
            #[wasm_bindgen(js_name = intoCoordType)]
            pub fn into_coord_type(self, coord_type: CoordType) -> Self {
                self.0.into_coord_type(coord_type.into()).into()
            }
        }
    };
}

impl_coord_type!(PointData);
impl_coord_type!(LineStringData);
impl_coord_type!(PolygonData);
impl_coord_type!(MultiPointData);
impl_coord_type!(MultiLineStringData);
impl_coord_type!(MultiPolygonData);
