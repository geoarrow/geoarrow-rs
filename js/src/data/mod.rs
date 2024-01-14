pub mod coord;

use arrow_array::BinaryArray;
pub use coord::{CoordBuffer, InterleavedCoordBuffer, SeparatedCoordBuffer};

use crate::error::WasmResult;
use crate::utils::vec_to_offsets;
use wasm_bindgen::prelude::*;

/// An enum of geometry types
#[wasm_bindgen]
pub enum GeometryType {
    Point = 0,
    LineString = 1,
    Polygon = 3,
    MultiPoint = 4,
    MultiLineString = 5,
    MultiPolygon = 6,
}

macro_rules! impl_data {
    (
        $(#[$($attrss:meta)*])*
        pub struct $struct_name:ident(pub(crate) $geoarrow_arr:ty);
    ) => {
        $(#[$($attrss)*])*
        #[wasm_bindgen]
        pub struct $struct_name(pub(crate) $geoarrow_arr);

        impl From<$geoarrow_arr> for $struct_name {
            fn from(value: $geoarrow_arr) -> Self {
                Self(value)
            }
        }

        impl From<$struct_name> for $geoarrow_arr {
            fn from(value: $struct_name) -> Self {
                value.0
            }
        }
    };
}

impl_data! {
    /// An immutable array of Point geometries in WebAssembly memory using GeoArrow's in-memory
    /// representation.
    pub struct PointData(pub(crate) geoarrow::array::PointArray);
}
impl_data! {
    /// An immutable array of LineString geometries in WebAssembly memory using GeoArrow's
    /// in-memory representation.
    pub struct LineStringData(pub(crate) geoarrow::array::LineStringArray<i32>);
}
impl_data! {
    /// An immutable array of Polygon geometries in WebAssembly memory using GeoArrow's
    /// in-memory representation.
    pub struct PolygonData(pub(crate) geoarrow::array::PolygonArray<i32>);
}
impl_data! {
    /// An immutable array of MultiPoint geometries in WebAssembly memory using GeoArrow's
    /// in-memory representation.
    pub struct MultiPointData(pub(crate) geoarrow::array::MultiPointArray<i32>);
}
impl_data! {
    /// An immutable array of MultiLineString geometries in WebAssembly memory using GeoArrow's
    /// in-memory representation.
    pub struct MultiLineStringData(pub(crate) geoarrow::array::MultiLineStringArray<i32>);
}
impl_data! {
    /// An immutable array of MultiPolygon geometries in WebAssembly memory using GeoArrow's
    /// in-memory representation.
    pub struct MultiPolygonData(pub(crate) geoarrow::array::MultiPolygonArray<i32>);
}
impl_data! {
    /// An immutable array of Geometry geometries in WebAssembly memory using GeoArrow's
    /// in-memory representation.
    pub struct MixedGeometryData(pub(crate) geoarrow::array::MixedGeometryArray<i32>);
}
impl_data! {
    /// An immutable array of GeometryCollection geometries in WebAssembly memory using GeoArrow's
    /// in-memory representation.
    pub struct GeometryCollectionData(pub(crate) geoarrow::array::GeometryCollectionArray<i32>);
}
impl_data! {
    /// An immutable array of WKB-encoded geometries in WebAssembly memory using GeoArrow's
    /// in-memory representation.
    pub struct WKBData(pub(crate) geoarrow::array::WKBArray<i32>);
}
impl_data! {
    /// An immutable array of Rect geometries in WebAssembly memory using GeoArrow's
    /// in-memory representation.
    pub struct RectData(pub(crate) geoarrow::array::RectArray);
}

#[wasm_bindgen]
impl PointData {
    #[wasm_bindgen(constructor)]
    pub fn new(coords: CoordBuffer) -> Self {
        Self(geoarrow::array::PointArray::new(
            coords.0,
            None,
            Default::default(),
        ))
    }
}

#[wasm_bindgen]
impl LineStringData {
    #[wasm_bindgen(constructor)]
    pub fn new(coords: CoordBuffer, geom_offsets: Vec<i32>) -> Self {
        Self(geoarrow::array::LineStringArray::new(
            coords.0,
            vec_to_offsets(geom_offsets),
            None,
            Default::default(),
        ))
    }
}

#[wasm_bindgen]
impl PolygonData {
    #[wasm_bindgen(constructor)]
    pub fn new(
        coords: CoordBuffer,
        geom_offsets: Vec<i32>,
        ring_offsets: Vec<i32>,
        // validity: Option<BooleanArray>,
    ) -> Self {
        Self(geoarrow::array::PolygonArray::new(
            coords.0,
            vec_to_offsets(geom_offsets),
            vec_to_offsets(ring_offsets),
            None,
            Default::default(),
        ))
    }
}

#[wasm_bindgen]
impl MultiPointData {
    #[wasm_bindgen(constructor)]
    pub fn new(coords: CoordBuffer, geom_offsets: Vec<i32>) -> Self {
        Self(geoarrow::array::MultiPointArray::new(
            coords.0,
            vec_to_offsets(geom_offsets),
            None,
            Default::default(),
        ))
    }
}

#[wasm_bindgen]
impl MultiLineStringData {
    #[wasm_bindgen(constructor)]
    pub fn new(coords: CoordBuffer, geom_offsets: Vec<i32>, ring_offsets: Vec<i32>) -> Self {
        Self(geoarrow::array::MultiLineStringArray::new(
            coords.0,
            vec_to_offsets(geom_offsets),
            vec_to_offsets(ring_offsets),
            None,
            Default::default(),
        ))
    }
}

#[wasm_bindgen]
impl MultiPolygonData {
    #[wasm_bindgen(constructor)]
    pub fn new(
        coords: CoordBuffer,
        geom_offsets: Vec<i32>,
        polygon_offsets: Vec<i32>,
        ring_offsets: Vec<i32>,
    ) -> Self {
        Self(geoarrow::array::MultiPolygonArray::new(
            coords.0,
            vec_to_offsets(geom_offsets),
            vec_to_offsets(polygon_offsets),
            vec_to_offsets(ring_offsets),
            None,
            Default::default(),
        ))
    }
}

#[wasm_bindgen]
impl WKBData {
    #[wasm_bindgen(constructor)]
    pub fn new(values: Vec<u8>, offsets: Vec<i32>) -> Self {
        let binary_array = BinaryArray::new(vec_to_offsets(offsets), values.into(), None);

        Self(geoarrow::array::WKBArray::new(
            binary_array,
            Default::default(),
        ))
    }

    /// Convert this WKBData into a PointArray
    ///
    /// ## Memory management
    ///
    /// This operation consumes and neuters the existing WKBData, so it will no longer be valid
    /// and the original wkb array's memory does not need to be freed manually.
    #[wasm_bindgen(js_name = intoPointArray)]
    pub fn into_point_array(self) -> WasmResult<PointData> {
        let arr: geoarrow::array::PointArray = self.0.try_into().unwrap();
        Ok(arr.into())
    }

    /// Convert this WKBData into a LineStringArray
    ///
    /// ## Memory management
    ///
    /// This operation consumes and neuters the existing WKBData, so it will no longer be valid
    /// and the original wkb array's memory does not need to be freed manually.
    #[wasm_bindgen(js_name = intoLineStringArray)]
    pub fn into_line_string_array(self) -> WasmResult<LineStringData> {
        let arr: geoarrow::array::LineStringArray<i32> = self.0.try_into().unwrap();
        Ok(arr.into())
    }

    /// Convert this WKBData into a PolygonArray
    ///
    /// ## Memory management
    ///
    /// This operation consumes and neuters the existing WKBData, so it will no longer be valid
    /// and the original wkb array's memory does not need to be freed manually.
    #[wasm_bindgen(js_name = intoPolygonArray)]
    pub fn into_polygon_array(self) -> WasmResult<PolygonData> {
        let arr: geoarrow::array::PolygonArray<i32> = self.0.try_into().unwrap();
        Ok(arr.into())
    }

    /// Convert this WKBData into a MultiPointArray
    ///
    /// ## Memory management
    ///
    /// This operation consumes and neuters the existing WKBData, so it will no longer be valid
    /// and the original wkb array's memory does not need to be freed manually.
    #[wasm_bindgen(js_name = intoMultiPointArray)]
    pub fn into_multi_point_array(self) -> WasmResult<MultiPointData> {
        let arr: geoarrow::array::MultiPointArray<i32> = self.0.try_into().unwrap();
        Ok(arr.into())
    }

    /// Convert this WKBData into a MultiLineStringArray
    ///
    /// ## Memory management
    ///
    /// This operation consumes and neuters the existing WKBData, so it will no longer be valid
    /// and the original wkb array's memory does not need to be freed manually.
    #[wasm_bindgen(js_name = intoMultiLineStringArray)]
    pub fn into_multi_line_string_array(self) -> WasmResult<MultiLineStringData> {
        let arr: geoarrow::array::MultiLineStringArray<i32> = self.0.try_into().unwrap();
        Ok(arr.into())
    }

    /// Convert this WKBData into a MultiPolygonArray
    ///
    /// ## Memory management
    ///
    /// This operation consumes and neuters the existing WKBData, so it will no longer be valid
    /// and the original wkb array's memory does not need to be freed manually.
    #[wasm_bindgen(js_name = intoMultiPolygonArray)]
    pub fn into_multi_polygon_array(self) -> WasmResult<MultiPolygonData> {
        let arr: geoarrow::array::MultiPolygonArray<i32> = self.0.try_into().unwrap();
        Ok(arr.into())
    }
}
