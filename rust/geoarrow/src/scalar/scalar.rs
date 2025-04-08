use geoarrow_schema::Dimension;

use crate::array::{
    AsNativeArray, GeometryArray, GeometryCollectionArray, LineStringArray, MultiLineStringArray,
    MultiPointArray, MultiPolygonArray, PointArray, PolygonArray, RectArray,
};
use crate::datatypes::NativeType;
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
    /// Create a new scalar from an array of length 1
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

    /// The data type of this scalar
    pub fn data_type(&self) -> NativeType {
        self.0.data_type()
    }

    /// Access the underlying array.
    ///
    /// The array will have a single element.
    pub fn inner(&self) -> &NativeArrayRef {
        &self.0
    }

    /// Access the underlying array.
    ///
    /// The array will have a single element.
    pub fn into_inner(self) -> NativeArrayRef {
        self.0
    }

    /// Access the underlying dimension
    ///
    /// If the type of the array is `Geometry`, dimension will be `None`.
    pub fn dimension(&self) -> Option<Dimension> {
        self.data_type().dimension()
    }

    /// Convert to a [Geometry]
    pub fn as_geometry(&self) -> Option<Geometry<'_>> {
        // Note: we use `.downcast_ref` directly here because we need to pass in the generic
        // TODO: may be able to change this now that we don't have <O>
        //
        // TODO: as of Nov 2024 we should be able to switch back to the downcasting helpers

        // Note: switching to the cast helpers using as_ref creates a temporary value. We'll have
        // to work around that.

        match self.data_type() {
            NativeType::Point(_) => {
                let arr = self.0.as_any().downcast_ref::<PointArray>().unwrap();
                arr.get(0).map(Geometry::Point)
            }
            NativeType::LineString(_) => {
                let arr = self.0.as_any().downcast_ref::<LineStringArray>().unwrap();
                arr.get(0).map(Geometry::LineString)
            }
            NativeType::Polygon(_) => {
                let arr = self.0.as_any().downcast_ref::<PolygonArray>().unwrap();
                arr.get(0).map(Geometry::Polygon)
            }
            NativeType::MultiPoint(_) => {
                let arr = self.0.as_any().downcast_ref::<MultiPointArray>().unwrap();
                arr.get(0).map(Geometry::MultiPoint)
            }
            NativeType::MultiLineString(_) => {
                let arr = self
                    .0
                    .as_any()
                    .downcast_ref::<MultiLineStringArray>()
                    .unwrap();
                arr.get(0).map(Geometry::MultiLineString)
            }
            NativeType::MultiPolygon(_) => {
                let arr = self.0.as_any().downcast_ref::<MultiPolygonArray>().unwrap();
                arr.get(0).map(Geometry::MultiPolygon)
            }
            NativeType::GeometryCollection(_) => {
                let arr = self
                    .0
                    .as_any()
                    .downcast_ref::<GeometryCollectionArray>()
                    .unwrap();
                arr.get(0).map(Geometry::GeometryCollection)
            }
            NativeType::Rect(_) => {
                let arr = self.0.as_any().downcast_ref::<RectArray>().unwrap();
                arr.get(0).map(Geometry::Rect)
            }
            NativeType::Geometry(_) => {
                let arr = self.0.as_any().downcast_ref::<GeometryArray>().unwrap();
                arr.get(0)
            }
        }
    }

    /// Convert to a [geo::Geometry].
    pub fn to_geo(&self) -> geo::Geometry {
        macro_rules! impl_to_geo {
            ($cast_func:ident) => {{ self.0.as_ref().$cast_func().value(0).to_geo_geometry() }};
        }

        use NativeType::*;

        match self.data_type() {
            Point(_) => impl_to_geo!(as_point),
            LineString(_) => impl_to_geo!(as_line_string),
            Polygon(_) => impl_to_geo!(as_polygon),
            MultiPoint(_) => impl_to_geo!(as_multi_point),
            MultiLineString(_) => impl_to_geo!(as_multi_line_string),
            MultiPolygon(_) => impl_to_geo!(as_multi_polygon),
            GeometryCollection(_) => impl_to_geo!(as_geometry_collection),
            Rect(_) => impl_to_geo!(as_rect),
            Geometry(_) => impl_to_geo!(as_geometry),
        }
    }

    /// Convert to a [geo::Point].
    pub fn to_geo_point(&self) -> Result<geo::Point> {
        match self.to_geo() {
            geo::Geometry::Point(g) => Ok(g),
            dt => Err(GeoArrowError::General(format!(
                "Expected Point, got {:?}",
                dt
            ))),
        }
    }

    /// Convert to a [geo::LineString].
    pub fn to_geo_line_string(&self) -> Result<geo::LineString> {
        match self.to_geo() {
            geo::Geometry::LineString(g) => Ok(g),
            dt => Err(GeoArrowError::General(format!(
                "Expected LineString, got {:?}",
                dt
            ))),
        }
    }

    /// Convert to a [geo::Polygon].
    pub fn to_geo_polygon(&self) -> Result<geo::Polygon> {
        match self.to_geo() {
            geo::Geometry::Polygon(g) => Ok(g),
            dt => Err(GeoArrowError::General(format!(
                "Expected Polygon, got {:?}",
                dt
            ))),
        }
    }

    /// Convert to a [geo::MultiPoint].
    pub fn to_geo_multi_point(&self) -> Result<geo::MultiPoint> {
        match self.to_geo() {
            geo::Geometry::MultiPoint(g) => Ok(g),
            dt => Err(GeoArrowError::General(format!(
                "Expected MultiPoint, got {:?}",
                dt
            ))),
        }
    }

    /// Convert to a [geo::MultiLineString].
    pub fn to_geo_multi_line_string(&self) -> Result<geo::MultiLineString> {
        match self.to_geo() {
            geo::Geometry::MultiLineString(g) => Ok(g),
            dt => Err(GeoArrowError::General(format!(
                "Expected MultiLineString, got {:?}",
                dt
            ))),
        }
    }

    /// Convert to a [geo::MultiPolygon].
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
//             Point(_) => {
//                 let arr = self.0.as_ref().as_point::<2>();

//                 arr.get(0).map(Geometry::Point)

//                 AnyPoint::XY( self.as_geometry::<>())
//             }

//         }
//     }
// }
