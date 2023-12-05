use crate::array::*;
use crate::error::Result;
use crate::GeometryArrayTrait;
use proj::{Proj, Transform};

/// Reproject an array using PROJ
pub trait Reproject {
    fn reproject(&self, proj: &Proj) -> Result<Self>
    where
        Self: Sized;
}

impl Reproject for PointArray {
    fn reproject(&self, proj: &Proj) -> Result<Self> {
        let mut output_array = PointBuilder::with_capacity(self.len());

        for maybe_geom in self.iter_geo() {
            if let Some(mut geom) = maybe_geom {
                geom.transform(proj)?;
                output_array.push_point(Some(&geom));
            } else {
                output_array.push_null()
            }
        }

        Ok(output_array.into())
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
        let out = point_array.reproject(&proj).unwrap();
        assert_eq!(out.value_as_geo(0).x(), 0.0);
        assert_relative_eq!(out.value_as_geo(0).y(), 111325.1428663851);
        dbg!(out);
    }
}
