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
        let mut output_array = MutablePointArray::with_capacity(self.len());

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
