use arrow_array::OffsetSizeTrait;

use crate::array::{AsGeometryArray, PointArray};
use crate::datatypes::{Dimension, GeoDataType};
use crate::error::{GeoArrowError, Result};
use crate::io::geo::geometry_to_geo;
use crate::scalar::Geometry;
use crate::trait_::{GeometryArrayAccessor, GeometryArrayRef, GeometryScalarTrait};

/// A dynamically typed GeoArrow scalar
///
/// Note: this name will probably be changed in the future.
///
/// This stores an `Arc<dyn GeometryArrayTrait>` that has a single value.
#[derive(Debug, Clone)]
pub struct GeometryScalar(GeometryArrayRef);

impl GeometryScalar {
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

    // pub fn as_geometry<O: OffsetSizeTrait, const D: usize>(&self) -> Geometry<'_, O, D> {
    //     use GeoDataType::*;

    //     match self.data_type() {
    //         Point(_, _) => {
    //             self.0.as_ref().as_point::<2>()
    //             let arr = self.0.as_any().downcast_ref::<PointArray<D>>().unwrap();
    //             Geometry::Point(arr.value(0))
    //         }
    //     }

    //     if D == 2 {
    //         match self.data_type() {
    //             Point(_, _) => {
    //                 let arr = self.0.as_any().downcast_ref::<PointArray<D>>().unwrap();
    //                 Geometry::Point(arr.value(0))
    //             }
    //             // LineString(_, XY) => impl_process!(as_line_string_2d),
    //             // LargeLineString(_, XY) => {
    //             //     impl_process!(as_large_line_string_2d)
    //             // }
    //             // Polygon(_, XY) => impl_process!(true, as_polygon_2d),
    //             // LargePolygon(_, XY) => impl_process!(true, as_large_polygon_2d),
    //             // MultiPoint(_, XY) => impl_process!(as_multi_point_2d),
    //             // LargeMultiPoint(_, XY) => {
    //             //     impl_process!(as_large_multi_point_2d)
    //             // }
    //             // MultiLineString(_, XY) => {
    //             //     impl_process!(as_multi_line_string_2d)
    //             // }
    //             // LargeMultiLineString(_, XY) => {
    //             //     impl_process!(as_large_multi_line_string_2d)
    //             // }
    //             // MultiPolygon(_, XY) => impl_process!(as_multi_polygon_2d),
    //             // LargeMultiPolygon(_, XY) => {
    //             //     impl_process!(as_large_multi_polygon_2d)
    //             // }
    //             // Mixed(_, XY) => impl_process!(as_mixed_2d),
    //             // LargeMixed(_, XY) => impl_process!(as_large_mixed_2d),
    //             // GeometryCollection(_, XY) => {
    //             //     impl_process!(as_geometry_collection_2d)
    //             // }
    //             // LargeGeometryCollection(_, XY) => {
    //             //     impl_process!(as_large_geometry_collection_2d)
    //             // }
    //             _ => todo!(),
    //         }
    //     } else {
    //         todo!()
    //     }
    // }

    pub fn to_geo(&self) -> geo::Geometry {
        macro_rules! impl_to_geo {
            ($cast_func:ident, $dim:expr) => {{
                self.0
                    .as_ref()
                    .$cast_func::<$dim>()
                    .value(0)
                    .to_geo_geometry()
            }};
        }

        use Dimension::*;
        use GeoDataType::*;

        match self.data_type() {
            Point(_, XY) => impl_to_geo!(as_point, 2),
            LineString(_, XY) => impl_to_geo!(as_line_string, 2),
            LargeLineString(_, XY) => impl_to_geo!(as_large_line_string, 2),
            Polygon(_, XY) => impl_to_geo!(as_polygon, 2),
            LargePolygon(_, XY) => impl_to_geo!(as_large_polygon, 2),
            MultiPoint(_, XY) => impl_to_geo!(as_multi_point, 2),
            LargeMultiPoint(_, XY) => impl_to_geo!(as_large_multi_point, 2),
            MultiLineString(_, XY) => {
                impl_to_geo!(as_multi_line_string, 2)
            }
            LargeMultiLineString(_, XY) => {
                impl_to_geo!(as_large_multi_line_string, 2)
            }
            MultiPolygon(_, XY) => impl_to_geo!(as_multi_polygon, 2),
            LargeMultiPolygon(_, XY) => {
                impl_to_geo!(as_large_multi_polygon, 2)
            }
            Mixed(_, XY) => impl_to_geo!(as_mixed, 2),
            LargeMixed(_, XY) => impl_to_geo!(as_large_mixed, 2),
            GeometryCollection(_, XY) => {
                impl_to_geo!(as_geometry_collection, 2)
            }
            LargeGeometryCollection(_, XY) => {
                impl_to_geo!(as_large_geometry_collection, 2)
            }
            Rect(XY) => impl_to_geo!(as_rect, 2),
            Point(_, XYZ) => impl_to_geo!(as_point, 3),
            LineString(_, XYZ) => impl_to_geo!(as_line_string, 3),
            LargeLineString(_, XYZ) => impl_to_geo!(as_large_line_string, 3),
            Polygon(_, XYZ) => impl_to_geo!(as_polygon, 3),
            LargePolygon(_, XYZ) => impl_to_geo!(as_large_polygon, 3),
            MultiPoint(_, XYZ) => impl_to_geo!(as_multi_point, 3),
            LargeMultiPoint(_, XYZ) => impl_to_geo!(as_large_multi_point, 3),
            MultiLineString(_, XYZ) => {
                impl_to_geo!(as_multi_line_string, 3)
            }
            LargeMultiLineString(_, XYZ) => {
                impl_to_geo!(as_large_multi_line_string, 3)
            }
            MultiPolygon(_, XYZ) => impl_to_geo!(as_multi_polygon, 3),
            LargeMultiPolygon(_, XYZ) => {
                impl_to_geo!(as_large_multi_polygon, 3)
            }
            Mixed(_, XYZ) => impl_to_geo!(as_mixed, 3),
            LargeMixed(_, XYZ) => impl_to_geo!(as_large_mixed, 3),
            GeometryCollection(_, XYZ) => {
                impl_to_geo!(as_geometry_collection, 3)
            }
            LargeGeometryCollection(_, XYZ) => {
                impl_to_geo!(as_large_geometry_collection, 3)
            }
            Rect(XYZ) => impl_to_geo!(as_rect, 3),
            WKB => {
                let arr = self.0.as_ref();
                let wkb_arr = arr.as_wkb().value(0);
                let wkb_object = wkb_arr.to_wkb_object();
                geometry_to_geo(&wkb_object)
            }
            LargeWKB => {
                let arr = self.0.as_ref();
                let wkb_arr = arr.as_large_wkb().value(0);
                let wkb_object = wkb_arr.to_wkb_object();
                geometry_to_geo(&wkb_object)
            }
        }
    }

    pub fn to_geo_point(&self) -> Result<geo::Point> {
        match self.to_geo() {
            geo::Geometry::Point(g) => Ok(g),
            dt => Err(GeoArrowError::General(format!(
                "Expected Point, got {:?}",
                dt
            ))),
        }
    }

    pub fn to_geo_line_string(&self) -> Result<geo::LineString> {
        match self.to_geo() {
            geo::Geometry::LineString(g) => Ok(g),
            dt => Err(GeoArrowError::General(format!(
                "Expected LineString, got {:?}",
                dt
            ))),
        }
    }

    pub fn to_geo_polygon(&self) -> Result<geo::Polygon> {
        match self.to_geo() {
            geo::Geometry::Polygon(g) => Ok(g),
            dt => Err(GeoArrowError::General(format!(
                "Expected Polygon, got {:?}",
                dt
            ))),
        }
    }

    pub fn to_geo_multi_point(&self) -> Result<geo::MultiPoint> {
        match self.to_geo() {
            geo::Geometry::MultiPoint(g) => Ok(g),
            dt => Err(GeoArrowError::General(format!(
                "Expected MultiPoint, got {:?}",
                dt
            ))),
        }
    }

    pub fn to_geo_multi_line_string(&self) -> Result<geo::MultiLineString> {
        match self.to_geo() {
            geo::Geometry::MultiLineString(g) => Ok(g),
            dt => Err(GeoArrowError::General(format!(
                "Expected MultiLineString, got {:?}",
                dt
            ))),
        }
    }

    pub fn to_geo_multi_polygon(&self) -> Result<geo::MultiPolygon> {
        match self.to_geo() {
            geo::Geometry::MultiPolygon(g) => Ok(g),
            dt => Err(GeoArrowError::General(format!(
                "Expected MultiPolygon, got {:?}",
                dt
            ))),
        }
    }
}
