use crate::array::{
    AsNativeArray, GeometryCollectionArray, LineStringArray, MixedGeometryArray,
    MultiLineStringArray, MultiPointArray, MultiPolygonArray, PointArray, PolygonArray, RectArray,
};
use crate::datatypes::{Dimension, NativeType};
use crate::error::{GeoArrowError, Result};
use crate::scalar::Geometry;
use crate::trait_::{ArrayAccessor, NativeArrayRef, NativeScalar};

/// A dynamically typed GeoArrow scalar
///
/// Note: this name will probably be changed in the future.
///
/// This stores an `Arc<dyn NativeArray>` that has a single value.
#[derive(Debug, Clone)]
pub struct GeometryScalar(NativeArrayRef);

impl GeometryScalar {
    pub fn try_new(array: NativeArrayRef) -> Result<Self> {
        if array.len() != 1 {
            Err(GeoArrowError::General(format!(
                "Expected array with length 1, got {}",
                array.len()
            )))
        } else {
            Ok(Self(array))
        }
    }

    pub fn data_type(&self) -> NativeType {
        self.0.data_type()
    }

    pub fn inner(&self) -> &NativeArrayRef {
        &self.0
    }

    pub fn into_inner(self) -> NativeArrayRef {
        self.0
    }

    pub fn dimension(&self) -> Dimension {
        use NativeType::*;
        match self.data_type() {
            Point(_, dim)
            | LineString(_, dim)
            | Polygon(_, dim)
            | MultiPoint(_, dim)
            | MultiLineString(_, dim)
            | MultiPolygon(_, dim)
            | Mixed(_, dim)
            | GeometryCollection(_, dim)
            | Rect(dim) => dim,
            // WKB => {
            //     let arr = self.0.as_ref();
            //     let wkb_arr = arr.as_wkb().value(0);
            //     let wkb_obj = wkb_arr.to_wkb_object();
            //     wkb_obj.dimension()
            // }
            // LargeWKB => {
            //     let arr = self.0.as_ref();
            //     let wkb_arr = arr.as_large_wkb().value(0);
            //     let wkb_obj = wkb_arr.to_wkb_object();
            //     wkb_obj.dimension()
            // }
        }
    }

    pub fn as_geometry(&self) -> Option<Geometry<'_>> {
        use NativeType::*;

        // Note: we use `.downcast_ref` directly here because we need to pass in the generic
        // TODO: may be able to change this now that we don't have <O>
        match self.data_type() {
            Point(_, _) => {
                let arr = self.0.as_any().downcast_ref::<PointArray<D>>().unwrap();
                arr.get(0).map(Geometry::Point)
            }
            LineString(_, _) => {
                let arr = self
                    .0
                    .as_any()
                    .downcast_ref::<LineStringArray<D>>()
                    .unwrap();
                arr.get(0).map(Geometry::LineString)
            }
            Polygon(_, _) => {
                let arr = self.0.as_any().downcast_ref::<PolygonArray<D>>().unwrap();
                arr.get(0).map(Geometry::Polygon)
            }
            MultiPoint(_, _) => {
                let arr = self
                    .0
                    .as_any()
                    .downcast_ref::<MultiPointArray<D>>()
                    .unwrap();
                arr.get(0).map(Geometry::MultiPoint)
            }
            MultiLineString(_, _) => {
                let arr = self
                    .0
                    .as_any()
                    .downcast_ref::<MultiLineStringArray<D>>()
                    .unwrap();
                arr.get(0).map(Geometry::MultiLineString)
            }
            MultiPolygon(_, _) => {
                let arr = self
                    .0
                    .as_any()
                    .downcast_ref::<MultiPolygonArray<D>>()
                    .unwrap();
                arr.get(0).map(Geometry::MultiPolygon)
            }
            Mixed(_, _) => {
                let arr = self
                    .0
                    .as_any()
                    .downcast_ref::<MixedGeometryArray<D>>()
                    .unwrap();
                arr.get(0)
            }
            GeometryCollection(_, _) => {
                let arr = self
                    .0
                    .as_any()
                    .downcast_ref::<GeometryCollectionArray<D>>()
                    .unwrap();
                arr.get(0).map(Geometry::GeometryCollection)
            }
            Rect(_) => {
                let arr = self.0.as_any().downcast_ref::<RectArray<D>>().unwrap();
                arr.get(0).map(Geometry::Rect)
            }
        }
    }

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
        use NativeType::*;

        match self.data_type() {
            Point(_, XY) => impl_to_geo!(as_point, 2),
            LineString(_, XY) => impl_to_geo!(as_line_string, 2),
            Polygon(_, XY) => impl_to_geo!(as_polygon, 2),
            MultiPoint(_, XY) => impl_to_geo!(as_multi_point, 2),
            MultiLineString(_, XY) => {
                impl_to_geo!(as_multi_line_string, 2)
            }
            MultiPolygon(_, XY) => impl_to_geo!(as_multi_polygon, 2),
            Mixed(_, XY) => impl_to_geo!(as_mixed, 2),
            GeometryCollection(_, XY) => {
                impl_to_geo!(as_geometry_collection, 2)
            }
            Rect(XY) => impl_to_geo!(as_rect, 2),
            Point(_, XYZ) => impl_to_geo!(as_point, 3),
            LineString(_, XYZ) => impl_to_geo!(as_line_string, 3),
            Polygon(_, XYZ) => impl_to_geo!(as_polygon, 3),
            MultiPoint(_, XYZ) => impl_to_geo!(as_multi_point, 3),
            MultiLineString(_, XYZ) => {
                impl_to_geo!(as_multi_line_string, 3)
            }
            MultiPolygon(_, XYZ) => impl_to_geo!(as_multi_polygon, 3),
            Mixed(_, XYZ) => impl_to_geo!(as_mixed, 3),
            GeometryCollection(_, XYZ) => {
                impl_to_geo!(as_geometry_collection, 3)
            }
            Rect(XYZ) => impl_to_geo!(as_rect, 3),
            // WKB => {
            //     let arr = self.0.as_ref();
            //     let wkb_arr = arr.as_wkb().value(0);
            //     let wkb_object = wkb_arr.to_wkb_object();
            //     geometry_to_geo(&wkb_object)
            // }
            // LargeWKB => {
            //     let arr = self.0.as_ref();
            //     let wkb_arr = arr.as_large_wkb().value(0);
            //     let wkb_object = wkb_arr.to_wkb_object();
            //     geometry_to_geo(&wkb_object)
            // }
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

// #[allow(clippy::upper_case_acronyms)]
// enum AnyPoint<'a> {
//     XY(Point<'a, 2>),
//     XYZ(Point<'a, 3>),
// }

// impl<'a> PointTrait for AnyPoint<'a> {
//     type T = f64;

//     fn dim(&self) -> geo_traits::Dimensions {
//         match self {
//             Self::XY(_) => 2,
//             Self::XYZ(_) => 3,
//         }
//     }

//     fn nth_unchecked(&self, n: usize) -> Self::T {
//         match self {
//             Self::XY(g) => g.nth_unchecked(n),
//             Self::XYZ(g) => g.nth_unchecked(n),
//         }
//     }

//     fn x(&self) -> Self::T {
//         match self {
//             Self::XY(g) => g.x(),
//             Self::XYZ(g) => g.x(),
//         }
//     }

//     fn y(&self) -> Self::T {
//         match self {
//             Self::XY(g) => g.y(),
//             Self::XYZ(g) => g.y(),
//         }
//     }
// }

// enum AnyLineString<'a> {}

// impl<'a> LineStringTrait for AnyLineString<'a> {}

// enum AnyPolygon<'a> {}

// impl<'a> PolygonTrait for AnyPolygon<'a> {}

// enum AnyMultiPoint<'a> {}

// impl<'a> MultiPointTrait for AnyMultiPoint<'a> {}

// enum AnyMultiLineString<'a> {}

// impl<'a> MultiLineStringTrait for AnyMultiLineString<'a> {}

// enum AnyMultiPolygon<'a> {}

// impl<'a> MultiPolygonTrait for AnyMultiPolygon<'a> {}

// enum AnyGeometryCollection<'a> {}

// impl<'a> GeometryCollectionTrait for AnyGeometryCollection<'a> {}

// enum AnyRect<'a> {}

// impl<'a> RectTrait for AnyRect<'a> {}

// impl GeometryTrait for GeometryScalar {
//     type T = f64;

//     type Point<'a> = AnyPoint<'a>;
//     type LineString<'a> = AnyLineString<'a>;
//     type Polygon<'a> = AnyPolygon<'a>;
//     type MultiPoint<'a> = AnyMultiPoint<'a>;
//     type MultiLineString<'a> = AnyMultiLineString<'a>;
//     type MultiPolygon<'a> = AnyMultiPolygon<'a>;
//     type GeometryCollection<'a> = AnyGeometryCollection<'a>;
//     type Rect<'a> = AnyRect<'a>;

//     fn dim(&self) -> geo_traits::Dimensions {
//         self.dimension().size()
//     }

//     fn as_type(
//         &self,
//     ) -> geo_traits::GeometryType<
//         '_,
//         Self::Point<'_>,
//         Self::LineString<'_>,
//         Self::Polygon<'_>,
//         Self::MultiPoint<'_>,
//         Self::MultiLineString<'_>,
//         Self::MultiPolygon<'_>,
//         Self::GeometryCollection<'_>,
//         Self::Rect<'_>,
//     > {
//         use Dimension::*;
//         use NativeType::*;

//         match self.data_type() {
//             Point(_, XY) => {
//                 let arr = self.0.as_ref().as_point::<2>();

//                 arr.get(0).map(Geometry::Point)

//                 AnyPoint::XY( self.as_geometry::<>())
//             }

//         }
//     }
// }
