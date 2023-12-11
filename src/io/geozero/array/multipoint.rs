use crate::array::multipoint::MultiPointCapacity;
use crate::array::{MultiPointArray, MultiPointBuilder};
use crate::io::geozero::scalar::multipoint::process_multi_point;
use crate::trait_::GeometryArrayAccessor;
use crate::GeometryArrayTrait;
use arrow_array::OffsetSizeTrait;
use geozero::{GeomProcessor, GeozeroGeometry};

impl<O: OffsetSizeTrait> GeozeroGeometry for MultiPointArray<O> {
    fn process_geom<P: GeomProcessor>(&self, processor: &mut P) -> geozero::error::Result<()>
    where
        Self: Sized,
    {
        let num_geometries = self.len();
        processor.geometrycollection_begin(num_geometries, 0)?;

        for geom_idx in 0..num_geometries {
            process_multi_point(&self.value(geom_idx), geom_idx, processor)?;
        }

        processor.geometrycollection_end(num_geometries - 1)?;
        Ok(())
    }
}

/// GeoZero trait to convert to GeoArrow MultiPointArray.
pub trait ToMultiPointArray<O: OffsetSizeTrait> {
    /// Convert to GeoArrow MultiPointArray
    fn to_multi_point_array(&self) -> geozero::error::Result<MultiPointArray<O>>;

    /// Convert to a GeoArrow MultiPointBuilder
    fn to_mutable_multi_point_array(&self) -> geozero::error::Result<MultiPointBuilder<O>>;
}

impl<T: GeozeroGeometry, O: OffsetSizeTrait> ToMultiPointArray<O> for T {
    fn to_multi_point_array(&self) -> geozero::error::Result<MultiPointArray<O>> {
        Ok(self.to_mutable_multi_point_array()?.into())
    }

    fn to_mutable_multi_point_array(&self) -> geozero::error::Result<MultiPointBuilder<O>> {
        let mut mutable_array = MultiPointBuilder::<O>::new();
        self.process_geom(&mut mutable_array)?;
        Ok(mutable_array)
    }
}

#[allow(unused_variables)]
impl<O: OffsetSizeTrait> GeomProcessor for MultiPointBuilder<O> {
    fn geometrycollection_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        let capacity = MultiPointCapacity::new(0, size);
        self.reserve(capacity);
        Ok(())
    }

    fn xy(&mut self, x: f64, y: f64, idx: usize) -> geozero::error::Result<()> {
        // # Safety:
        // This upholds invariants because we call try_push_length in multipoint_begin to ensure
        // offset arrays are correct.
        unsafe { self.push_xy(x, y).unwrap() }
        Ok(())
    }

    fn point_begin(&mut self, idx: usize) -> geozero::error::Result<()> {
        let capacity = MultiPointCapacity::new(1, 0);
        self.reserve(capacity);
        self.try_push_length(1).unwrap();
        Ok(())
    }

    fn point_end(&mut self, idx: usize) -> geozero::error::Result<()> {
        Ok(())
    }

    fn multipoint_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        let capacity = MultiPointCapacity::new(size, 0);
        self.reserve(capacity);
        self.try_push_length(size).unwrap();
        Ok(())
    }

    fn multipoint_end(&mut self, idx: usize) -> geozero::error::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test::multipoint::{mp0, mp1};
    use crate::trait_::GeometryArrayAccessor;
    use geo::Geometry;
    use geozero::error::Result;
    use geozero::ToWkt;

    #[test]
    fn geozero_process_geom() -> Result<()> {
        let arr: MultiPointArray<i64> = vec![mp0(), mp1()].as_slice().into();
        let wkt = arr.to_wkt()?;
        let expected = "GEOMETRYCOLLECTION(MULTIPOINT(0 1,1 2),MULTIPOINT(3 4,5 6))";
        assert_eq!(wkt, expected);
        Ok(())
    }

    #[test]
    fn from_geozero() -> Result<()> {
        let geo = Geometry::GeometryCollection(
            vec![mp0(), mp1()]
                .into_iter()
                .map(Geometry::MultiPoint)
                .collect(),
        );
        let multi_point_array: MultiPointArray<i32> = geo.to_multi_point_array().unwrap();
        assert_eq!(multi_point_array.value_as_geo(0), mp0());
        assert_eq!(multi_point_array.value_as_geo(1), mp1());
        Ok(())
    }
}
