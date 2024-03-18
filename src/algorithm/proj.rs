//! Bindings to the [`proj`] crate for coordinate reprojection.

use std::sync::Arc;

use crate::algorithm::native::{MapCoords, MapCoordsChunked};
use crate::array::*;
use crate::chunked_array::*;
use crate::datatypes::GeoDataType;
use crate::error::{GeoArrowError, Result};
use crate::trait_::GeometryScalarTrait;
use crate::GeometryArrayTrait;
use arrow_array::OffsetSizeTrait;
use proj::{Proj, ProjError, Transform as _Transform};

/// Transform an array using PROJ
pub trait Transform {
    type Output;

    fn transform(&self, proj: &Proj) -> Result<Self::Output>;
}

impl Transform for PointArray {
    type Output = Self;

    fn transform(&self, proj: &Proj) -> Result<Self::Output> {
        self.try_map_coords(|coord| {
            let mut coord = coord.to_geo();
            coord.transform(proj)?;
            Ok::<_, ProjError>(coord)
        })
    }
}

impl Transform for RectArray {
    type Output = Self;

    fn transform(&self, proj: &Proj) -> Result<Self::Output> {
        self.try_map_coords(|coord| {
            let mut coord = coord.to_geo();
            coord.transform(proj)?;
            Ok::<_, ProjError>(coord)
        })
    }
}

macro_rules! iter_geo_impl {
    ($type:ty, $builder_type:ty) => {
        impl<O: OffsetSizeTrait> Transform for $type {
            type Output = Self;

            fn transform(&self, proj: &Proj) -> Result<Self::Output> {
                self.try_map_coords(|coord| {
                    let mut coord = coord.to_geo();
                    coord.transform(proj)?;
                    Ok::<_, ProjError>(coord)
                })
            }
        }
    };
}

iter_geo_impl!(LineStringArray<O>, LineStringBuilder<O>);
iter_geo_impl!(PolygonArray<O>, PolygonBuilder<O>);
iter_geo_impl!(MultiPointArray<O>, MultiPointBuilder<O>);
iter_geo_impl!(MultiLineStringArray<O>, MultiLineStringBuilder<O>);
iter_geo_impl!(MultiPolygonArray<O>, MultiPolygonBuilder<O>);
iter_geo_impl!(MixedGeometryArray<O>, MixedGeometryBuilder<O>);
iter_geo_impl!(GeometryCollectionArray<O>, GeometryCollectionBuilder<O>);

impl Transform for &dyn GeometryArrayTrait {
    type Output = Arc<dyn GeometryArrayTrait>;

    fn transform(&self, proj: &Proj) -> Result<Self::Output> {
        let result: Arc<dyn GeometryArrayTrait> = match self.data_type() {
            GeoDataType::Point(_) => Arc::new(self.as_point().transform(proj)?),
            GeoDataType::LineString(_) => Arc::new(self.as_line_string().transform(proj)?),
            GeoDataType::LargeLineString(_) => {
                Arc::new(self.as_large_line_string().transform(proj)?)
            }
            GeoDataType::Polygon(_) => Arc::new(self.as_polygon().transform(proj)?),
            GeoDataType::LargePolygon(_) => Arc::new(self.as_large_polygon().transform(proj)?),
            GeoDataType::MultiPoint(_) => Arc::new(self.as_multi_point().transform(proj)?),
            GeoDataType::LargeMultiPoint(_) => {
                Arc::new(self.as_large_multi_point().transform(proj)?)
            }
            GeoDataType::MultiLineString(_) => {
                Arc::new(self.as_multi_line_string().transform(proj)?)
            }
            GeoDataType::LargeMultiLineString(_) => {
                Arc::new(self.as_large_multi_line_string().transform(proj)?)
            }
            GeoDataType::MultiPolygon(_) => Arc::new(self.as_multi_polygon().transform(proj)?),
            GeoDataType::LargeMultiPolygon(_) => {
                Arc::new(self.as_large_multi_polygon().transform(proj)?)
            }
            GeoDataType::Mixed(_) => Arc::new(self.as_mixed().transform(proj)?),
            GeoDataType::LargeMixed(_) => Arc::new(self.as_large_mixed().transform(proj)?),
            GeoDataType::GeometryCollection(_) => {
                Arc::new(self.as_geometry_collection().transform(proj)?)
            }
            GeoDataType::LargeGeometryCollection(_) => {
                Arc::new(self.as_large_geometry_collection().transform(proj)?)
            }
            GeoDataType::Rect => Arc::new(self.as_rect().transform(proj)?),
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}

impl Transform for ChunkedPointArray {
    type Output = Self;

    fn transform(&self, proj: &Proj) -> Result<Self::Output> {
        let definition = proj.def()?;
        self.try_map_coords_init(
            || Proj::new(definition.as_str()).unwrap(),
            |proj, coord| {
                let mut coord = coord.to_geo();
                coord.transform(proj)?;
                Ok::<_, ProjError>(coord)
            },
        )
    }
}

impl Transform for ChunkedRectArray {
    type Output = Self;

    fn transform(&self, proj: &Proj) -> Result<Self::Output> {
        let definition = proj.def()?;
        self.try_map_coords_init(
            || Proj::new(definition.as_str()).unwrap(),
            |proj, coord| {
                let mut coord = coord.to_geo();
                coord.transform(proj)?;
                Ok::<_, ProjError>(coord)
            },
        )
    }
}

macro_rules! impl_chunked {
    ($struct_name:ty) => {
        impl<O: OffsetSizeTrait> Transform for $struct_name {
            type Output = Self;

            fn transform(&self, proj: &Proj) -> Result<Self::Output> {
                let definition = proj.def()?;
                self.try_map_coords_init(
                    || Proj::new(definition.as_str()).unwrap(),
                    |proj, coord| {
                        let mut coord = coord.to_geo();
                        coord.transform(proj)?;
                        Ok::<_, ProjError>(coord)
                    },
                )
            }
        }
    };
}

impl_chunked!(ChunkedLineStringArray<O>);
impl_chunked!(ChunkedPolygonArray<O>);
impl_chunked!(ChunkedMultiPointArray<O>);
impl_chunked!(ChunkedMultiLineStringArray<O>);
impl_chunked!(ChunkedMultiPolygonArray<O>);
impl_chunked!(ChunkedMixedGeometryArray<O>);
impl_chunked!(ChunkedGeometryCollectionArray<O>);

impl Transform for &dyn ChunkedGeometryArrayTrait {
    type Output = Arc<dyn ChunkedGeometryArrayTrait>;

    fn transform(&self, proj: &Proj) -> Result<Self::Output> {
        let result: Arc<dyn ChunkedGeometryArrayTrait> = match self.data_type() {
            GeoDataType::Point(_) => Arc::new(self.as_point().transform(proj)?),
            GeoDataType::LineString(_) => Arc::new(self.as_line_string().transform(proj)?),
            GeoDataType::LargeLineString(_) => {
                Arc::new(self.as_large_line_string().transform(proj)?)
            }
            GeoDataType::Polygon(_) => Arc::new(self.as_polygon().transform(proj)?),
            GeoDataType::LargePolygon(_) => Arc::new(self.as_large_polygon().transform(proj)?),
            GeoDataType::MultiPoint(_) => Arc::new(self.as_multi_point().transform(proj)?),
            GeoDataType::LargeMultiPoint(_) => {
                Arc::new(self.as_large_multi_point().transform(proj)?)
            }
            GeoDataType::MultiLineString(_) => {
                Arc::new(self.as_multi_line_string().transform(proj)?)
            }
            GeoDataType::LargeMultiLineString(_) => {
                Arc::new(self.as_large_multi_line_string().transform(proj)?)
            }
            GeoDataType::MultiPolygon(_) => Arc::new(self.as_multi_polygon().transform(proj)?),
            GeoDataType::LargeMultiPolygon(_) => {
                Arc::new(self.as_large_multi_polygon().transform(proj)?)
            }
            GeoDataType::Mixed(_) => Arc::new(self.as_mixed().transform(proj)?),
            GeoDataType::LargeMixed(_) => Arc::new(self.as_large_mixed().transform(proj)?),
            GeoDataType::GeometryCollection(_) => {
                Arc::new(self.as_geometry_collection().transform(proj)?)
            }
            GeoDataType::LargeGeometryCollection(_) => {
                Arc::new(self.as_large_geometry_collection().transform(proj)?)
            }
            GeoDataType::Rect => Arc::new(self.as_rect().transform(proj)?),
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}

#[cfg(test)]
mod test {
    use crate::trait_::GeometryArrayAccessor;
    use approx::assert_relative_eq;

    use super::*;
    use crate::test::point::{p0, p1, p2};

    #[test]
    fn point_round_trip() {
        let point_array: PointArray = vec![Some(p0()), Some(p1()), Some(p2())].into();
        let proj = Proj::new_known_crs("EPSG:4326", "EPSG:3857", None).unwrap();

        // You can verify this with PROJ on the command line:
        // echo 1 0 | cs2cs EPSG:4326 EPSG:3857
        // 0.00	111325.14 0.00
        // Though note that cs2cs is using y/x for EPSG:4326
        let out = point_array.transform(&proj).unwrap();
        assert_eq!(out.value_as_geo(0).x(), 0.0);
        assert_relative_eq!(out.value_as_geo(0).y(), 111325.1428663851);
        dbg!(out);
    }
}
