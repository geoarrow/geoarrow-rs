use arrow_array::OffsetSizeTrait;

use crate::array::{AsGeometryArray, PointArray};
use crate::datatypes::{Dimension, GeoDataType};
use crate::error::{GeoArrowError, Result};
use crate::scalar::Geometry;
use crate::trait_::{GeometryArrayAccessor, GeometryArrayRef};

/// A dynamically typed GeoArrow scalar
///
/// Note: this name will probably be changed in the future.
///
/// This stores an `Arc<dyn GeometryArrayTrait>` that has a single value.
pub struct GeometryScalarArray(GeometryArrayRef);

impl GeometryScalarArray {
    pub fn try_new(array: GeometryArrayRef) -> Result<Self> {
        if array.len() != 1 {
            Err(GeoArrowError::General(format!(
                "Expected array with length 1, got {}",
                array.len()
            )))
        } else {
            Ok(Self(array))
        }
    }

    pub fn data_type(&self) -> GeoDataType {
        self.0.data_type()
    }

    pub fn inner(&self) -> &GeometryArrayRef {
        &self.0
    }

    pub fn into_inner(self) -> GeometryArrayRef {
        self.0
    }

    pub fn dimension(&self) -> Dimension {
        use GeoDataType::*;
        match self.data_type() {
            Point(_, dim)
            | LineString(_, dim)
            | LargeLineString(_, dim)
            | Polygon(_, dim)
            | LargePolygon(_, dim)
            | MultiPoint(_, dim)
            | LargeMultiPoint(_, dim)
            | MultiLineString(_, dim)
            | LargeMultiLineString(_, dim)
            | MultiPolygon(_, dim)
            | LargeMultiPolygon(_, dim)
            | Mixed(_, dim)
            | LargeMixed(_, dim)
            | GeometryCollection(_, dim)
            | LargeGeometryCollection(_, dim)
            | Rect(dim) => dim,
            WKB => {
                let arr = self.0.as_ref();
                let wkb_arr = arr.as_wkb().value(0);
                let wkb_obj = wkb_arr.to_wkb_object();
                wkb_obj.dimension()
            }
            LargeWKB => {
                let arr = self.0.as_ref();
                let wkb_arr = arr.as_large_wkb().value(0);
                let wkb_obj = wkb_arr.to_wkb_object();
                wkb_obj.dimension()
            }
        }
    }

    pub fn as_geometry<O: OffsetSizeTrait, const D: usize>(&self) -> Geometry<'_, O, D> {
        use GeoDataType::*;

        if D == 2 {
            match self.data_type() {
                Point(_, _) => {
                    let arr = self.0.as_any().downcast_ref::<PointArray<D>>().unwrap();
                    Geometry::Point(arr.value(0))
                }
                // LineString(_, XY) => impl_process!(as_line_string_2d),
                // LargeLineString(_, XY) => {
                //     impl_process!(as_large_line_string_2d)
                // }
                // Polygon(_, XY) => impl_process!(true, as_polygon_2d),
                // LargePolygon(_, XY) => impl_process!(true, as_large_polygon_2d),
                // MultiPoint(_, XY) => impl_process!(as_multi_point_2d),
                // LargeMultiPoint(_, XY) => {
                //     impl_process!(as_large_multi_point_2d)
                // }
                // MultiLineString(_, XY) => {
                //     impl_process!(as_multi_line_string_2d)
                // }
                // LargeMultiLineString(_, XY) => {
                //     impl_process!(as_large_multi_line_string_2d)
                // }
                // MultiPolygon(_, XY) => impl_process!(as_multi_polygon_2d),
                // LargeMultiPolygon(_, XY) => {
                //     impl_process!(as_large_multi_polygon_2d)
                // }
                // Mixed(_, XY) => impl_process!(as_mixed_2d),
                // LargeMixed(_, XY) => impl_process!(as_large_mixed_2d),
                // GeometryCollection(_, XY) => {
                //     impl_process!(as_geometry_collection_2d)
                // }
                // LargeGeometryCollection(_, XY) => {
                //     impl_process!(as_large_geometry_collection_2d)
                // }
                _ => todo!(),
            }
        } else {
            todo!()
        }
    }
}

// struct PointScalarArray(GeometryArrayRef);

// impl PointTrait for PointScalarArray {
//     type T = f64;

//     fn dim(&self) -> usize {
//         GeometryScalarArray(self.0.clone()).dimension().size()
//     }

//     fn x(&self) -> Self::T {
//         match self.0.data_type() {
//             GeoDataType::Point(_, Dimension::XY) => self.0.as_ref().as_point_2d().value(0).x(),
//             GeoDataType::Point(_, Dimension::XYZ) => self.0.as_ref().as_point_3d().value(0).x(),
//             _ => unreachable!(),
//         }
//     }
// }
