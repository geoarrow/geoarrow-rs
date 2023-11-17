use crate::array::{MutablePolygonArray, PolygonArray};
use crate::io::geozero::scalar::polygon::process_polygon;
use crate::trait_::GeoArrayAccessor;
use crate::GeometryArrayTrait;
use arrow_array::OffsetSizeTrait;
use geozero::{GeomProcessor, GeozeroGeometry};

impl<O: OffsetSizeTrait> GeozeroGeometry for PolygonArray<O> {
    fn process_geom<P: GeomProcessor>(&self, processor: &mut P) -> geozero::error::Result<()>
    where
        Self: Sized,
    {
        let num_geometries = self.len();
        processor.geometrycollection_begin(num_geometries, 0)?;

        for geom_idx in 0..num_geometries {
            process_polygon(&self.value(geom_idx), true, geom_idx, processor)?;
        }

        processor.geometrycollection_end(num_geometries - 1)?;
        Ok(())
    }
}

/// GeoZero trait to convert to GeoArrow PolygonArray.
pub trait ToGeoArrowPolygonArray<O: OffsetSizeTrait> {
    /// Convert to GeoArrow PolygonArray
    fn to_line_string_array(&self) -> geozero::error::Result<PolygonArray<O>>;

    /// Convert to a GeoArrow MutablePolygonArray
    fn to_mutable_line_string_array(&self) -> geozero::error::Result<MutablePolygonArray<O>>;
}

impl<T: GeozeroGeometry, O: OffsetSizeTrait> ToGeoArrowPolygonArray<O> for T {
    fn to_line_string_array(&self) -> geozero::error::Result<PolygonArray<O>> {
        Ok(self.to_mutable_line_string_array()?.into())
    }

    fn to_mutable_line_string_array(&self) -> geozero::error::Result<MutablePolygonArray<O>> {
        let mut mutable_array = MutablePolygonArray::<O>::new();
        self.process_geom(&mut mutable_array)?;
        Ok(mutable_array)
    }
}

#[allow(unused_variables)]
impl<O: OffsetSizeTrait> GeomProcessor for MutablePolygonArray<O> {
    fn geometrycollection_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        // reserve `size` geometries
        self.reserve(0, 0, size);
        Ok(())
    }

    fn geometrycollection_end(&mut self, idx: usize) -> geozero::error::Result<()> {
        // self.shrink_to_fit()
        Ok(())
    }

    fn xy(&mut self, x: f64, y: f64, idx: usize) -> geozero::error::Result<()> {
        // # Safety:
        // This upholds invariants because we call try_push_length in multipoint_begin to ensure
        // offset arrays are correct.
        unsafe { self.push_xy(x, y).unwrap() }
        Ok(())
    }

    // Here, size is the number of rings in the polygon
    fn polygon_begin(
        &mut self,
        tagged: bool,
        size: usize,
        idx: usize,
    ) -> geozero::error::Result<()> {
        // reserve `size` rings
        self.reserve(0, size, 0);

        // # Safety:
        // This upholds invariants because we separately update the ring offsets in
        // linestring_begin
        unsafe { self.try_push_geom_offset(size).unwrap() }
        Ok(())
    }

    fn linestring_begin(
        &mut self,
        tagged: bool,
        size: usize,
        idx: usize,
    ) -> geozero::error::Result<()> {
        // reserve `size` coordinates
        self.reserve(size, 0, 0);

        // # Safety:
        // This upholds invariants because we separately update the geometry offsets in
        // polygon_begin
        unsafe { self.try_push_ring_offset(size).unwrap() }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test::polygon::{p0, p1};
    use crate::trait_::GeoArrayAccessor;
    use geo::Geometry;
    use geozero::error::Result;
    use geozero::ToWkt;

    #[test]
    fn geozero_process_geom() -> geozero::error::Result<()> {
        let arr: PolygonArray<i64> = vec![p0(), p1()].into();
        let wkt = arr.to_wkt()?;
        let expected = "GEOMETRYCOLLECTION(POLYGON((-111 45,-111 41,-104 41,-104 45,-111 45)),POLYGON((-111 45,-111 41,-104 41,-104 45,-111 45),(-110 44,-110 42,-105 42,-105 44,-110 44)))";
        assert_eq!(wkt, expected);
        Ok(())
    }

    #[test]
    fn from_geozero() -> Result<()> {
        let geo = Geometry::GeometryCollection(
            vec![p0(), p1()]
                .into_iter()
                .map(Geometry::Polygon)
                .collect(),
        );
        let multi_point_array: PolygonArray<i32> = geo.to_line_string_array().unwrap();
        assert_eq!(multi_point_array.value_as_geo(0), p0());
        assert_eq!(multi_point_array.value_as_geo(1), p1());
        Ok(())
    }
}
