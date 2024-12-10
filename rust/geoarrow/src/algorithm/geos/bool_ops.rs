use arrow_array::BooleanArray;
use geo_traits::GeometryTrait;
use geos::Geom;

use crate::algorithm::native::{Binary, Unary};
use crate::array::GeometryArray;
use crate::error::{GeoArrowError, Result};
use crate::io::geos::scalar::{to_geos_geometry, GEOSGeometry};
use crate::trait_::NativeScalar;

pub trait BooleanOps<Rhs> {
    fn intersects(&self, rhs: &Rhs) -> Result<BooleanArray>;
    fn crosses(&self, rhs: &Rhs) -> Result<BooleanArray>;
    fn disjoint(&self, rhs: &Rhs) -> Result<BooleanArray>;
    fn touches(&self, rhs: &Rhs) -> Result<BooleanArray>;
    fn overlaps(&self, rhs: &Rhs) -> Result<BooleanArray>;
    fn within(&self, rhs: &Rhs) -> Result<BooleanArray>;
    fn equals(&self, rhs: &Rhs) -> Result<BooleanArray>;
    fn equals_exact(&self, rhs: &Rhs, precision: f64) -> Result<BooleanArray>;
    fn covers(&self, rhs: &Rhs) -> Result<BooleanArray>;
    fn covered_by(&self, rhs: &Rhs) -> Result<BooleanArray>;
    fn contains(&self, rhs: &Rhs) -> Result<BooleanArray>;

    fn difference(&self, rhs: &Rhs) -> Result<GeometryArray>;
    fn sym_difference(&self, rhs: &Rhs) -> Result<GeometryArray>;
    fn union(&self, rhs: &Rhs) -> Result<GeometryArray>;
    fn intersection(&self, rhs: &Rhs) -> Result<GeometryArray>;
}

macro_rules! impl_method {
    ($method_name:ident) => {
        fn $method_name(&self, rhs: &GeometryArray) -> Result<BooleanArray> {
            self.try_binary_boolean(rhs, |left, right| {
                Ok(left.to_geos()?.$method_name(&right.to_geos()?)?)
            })
        }
    };
}

macro_rules! impl_method_geometry {
    ($method_name:ident) => {
        fn $method_name(&self, rhs: &GeometryArray) -> Result<GeometryArray> {
            self.try_binary_geometry(
                rhs,
                |left, right| {
                    let left = to_geos_geometry(&left)?;
                    let right = to_geos_geometry(&right)?;
                    let out = left.difference(&right)?;
                    Ok(GEOSGeometry::new(out))
                },
                false,
            )
        }
    };
}

impl BooleanOps<GeometryArray> for GeometryArray {
    impl_method!(intersects);
    impl_method!(crosses);
    impl_method!(disjoint);
    impl_method!(touches);
    impl_method!(overlaps);
    impl_method!(within);
    impl_method!(equals);
    impl_method!(covers);
    impl_method!(covered_by);
    impl_method!(contains);

    fn equals_exact(&self, rhs: &GeometryArray, precision: f64) -> Result<BooleanArray> {
        self.try_binary_boolean(rhs, |left, right| {
            Ok(left.to_geos()?.equals_exact(&right.to_geos()?, precision)?)
        })
    }

    impl_method_geometry!(difference);
    impl_method_geometry!(sym_difference);
    impl_method_geometry!(union);
    impl_method_geometry!(intersection);
}

pub trait BooleanOpsScalar<Rhs> {
    fn intersects(&self, rhs: &Rhs) -> Result<BooleanArray>;
    fn crosses(&self, rhs: &Rhs) -> Result<BooleanArray>;
    fn disjoint(&self, rhs: &Rhs) -> Result<BooleanArray>;
    fn touches(&self, rhs: &Rhs) -> Result<BooleanArray>;
    fn overlaps(&self, rhs: &Rhs) -> Result<BooleanArray>;
    fn within(&self, rhs: &Rhs) -> Result<BooleanArray>;
    fn equals(&self, rhs: &Rhs) -> Result<BooleanArray>;
    fn equals_exact(&self, rhs: &Rhs, precision: f64) -> Result<BooleanArray>;
    fn covers(&self, rhs: &Rhs) -> Result<BooleanArray>;
    fn covered_by(&self, rhs: &Rhs) -> Result<BooleanArray>;
    fn contains(&self, rhs: &Rhs) -> Result<BooleanArray>;

    fn difference(&self, rhs: &Rhs) -> Result<GeometryArray>;
    fn sym_difference(&self, rhs: &Rhs) -> Result<GeometryArray>;
    fn union(&self, rhs: &Rhs) -> Result<GeometryArray>;
    fn intersection(&self, rhs: &Rhs) -> Result<GeometryArray>;
}

macro_rules! impl_method_scalar {
    ($method_name:ident) => {
        fn $method_name(&self, rhs: &G) -> Result<BooleanArray> {
            let rhs = to_geos_geometry(rhs)?;
            self.try_unary_boolean::<_, GeoArrowError>(|geom| {
                Ok(geom.to_geos()?.$method_name(&rhs)?)
            })
        }
    };
}

macro_rules! impl_method_geometry_scalar {
    ($method_name:ident) => {
        fn $method_name(&self, rhs: &G) -> Result<GeometryArray> {
            let rhs = to_geos_geometry(rhs)?;
            self.try_unary_geometry(
                |geom| {
                    let geom = to_geos_geometry(&geom)?;
                    let out = geom.$method_name(&rhs)?;
                    Ok(GEOSGeometry::new(out))
                },
                false,
            )
        }
    };
}
impl<G: GeometryTrait<T = f64>> BooleanOpsScalar<G> for GeometryArray {
    impl_method_scalar!(intersects);
    impl_method_scalar!(crosses);
    impl_method_scalar!(disjoint);
    impl_method_scalar!(touches);
    impl_method_scalar!(overlaps);
    impl_method_scalar!(within);
    impl_method_scalar!(equals);
    impl_method_scalar!(covers);
    impl_method_scalar!(covered_by);
    impl_method_scalar!(contains);

    fn equals_exact(&self, rhs: &G, precision: f64) -> Result<BooleanArray> {
        let rhs = to_geos_geometry(rhs)?;
        self.try_unary_boolean::<_, GeoArrowError>(|geom| {
            Ok(geom.to_geos()?.equals_exact(&rhs, precision)?)
        })
    }

    impl_method_geometry_scalar!(difference);
    impl_method_geometry_scalar!(sym_difference);
    impl_method_geometry_scalar!(union);
    impl_method_geometry_scalar!(intersection);
}
