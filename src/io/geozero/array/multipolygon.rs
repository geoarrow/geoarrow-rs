use arrow2::types::Offset;
use geozero::{GeomProcessor, GeozeroGeometry};

use crate::array::{MultiPolygonArray, MutableMultiPolygonArray};
use crate::io::geozero::scalar::multipolygon::process_multi_polygon;
use crate::GeometryArrayTrait;

impl<O: Offset> GeozeroGeometry for MultiPolygonArray<O> {
    fn process_geom<P: GeomProcessor>(&self, processor: &mut P) -> geozero::error::Result<()>
    where
        Self: Sized,
    {
        let num_geometries = self.len();
        processor.geometrycollection_begin(num_geometries, 0)?;

        for geom_idx in 0..num_geometries {
            process_multi_polygon(self.value(geom_idx), geom_idx, processor)?;
        }

        processor.geometrycollection_end(num_geometries - 1)?;
        Ok(())
    }
}

/// GeoZero trait to convert to GeoArrow MultiPolygonArray.
pub trait ToGeoArrowMultiPolygonArray<O: Offset> {
    /// Convert to GeoArrow MultiPolygonArray
    fn to_line_string_array(&self) -> geozero::error::Result<MultiPolygonArray<O>>;

    /// Convert to a GeoArrow MutableMultiPolygonArray
    fn to_mutable_line_string_array(&self) -> geozero::error::Result<MutableMultiPolygonArray<O>>;
}

impl<T: GeozeroGeometry, O: Offset> ToGeoArrowMultiPolygonArray<O> for T {
    fn to_line_string_array(&self) -> geozero::error::Result<MultiPolygonArray<O>> {
        Ok(self.to_mutable_line_string_array()?.into())
    }

    fn to_mutable_line_string_array(&self) -> geozero::error::Result<MutableMultiPolygonArray<O>> {
        let mut mutable_array = MutableMultiPolygonArray::<O>::new();
        self.process_geom(&mut mutable_array)?;
        Ok(mutable_array)
    }
}

#[allow(unused_variables)]
impl<O: Offset> GeomProcessor for MutableMultiPolygonArray<O> {
    fn geometrycollection_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        // reserve `size` geometries
        self.reserve(0, 0, 0, size);
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

    fn multipolygon_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        // reserve `size` polygons
        self.reserve(0, 0, size, 0);

        // # Safety:
        // This upholds invariants because we separately update the ring offsets in
        // linestring_begin
        unsafe { self.try_push_geom_offset(size).unwrap() }
        Ok(())
    }

    fn polygon_begin(
        &mut self,
        tagged: bool,
        size: usize,
        idx: usize,
    ) -> geozero::error::Result<()> {
        // > An untagged Polygon is part of a MultiPolygon
        if tagged {
            // reserve 1 polygon
            self.reserve(0, 0, 1, 0);

            // # Safety:
            // This upholds invariants because we separately update the ring offsets in
            // linestring_begin
            unsafe { self.try_push_geom_offset(1).unwrap() }
        }

        // reserve `size` rings
        self.reserve(0, size, 0, 0);

        // # Safety:
        // This upholds invariants because we separately update the geometry offsets in
        // polygon_begin
        unsafe { self.try_push_polygon_offset(size).unwrap() }
        Ok(())
    }

    fn linestring_begin(
        &mut self,
        tagged: bool,
        size: usize,
        idx: usize,
    ) -> geozero::error::Result<()> {
        assert!(!tagged);

        // reserve `size` coordinates
        self.reserve(size, 0, 0, 0);

        // # Safety:
        // This upholds invariants because we separately update the ring offsets in
        // linestring_begin
        unsafe { self.try_push_ring_offset(size).unwrap() }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test::multipolygon::{mp0, mp1};
    use geo::Geometry;
    use geozero::error::Result;
    use geozero::ToWkt;

    #[test]
    fn geozero_process_geom() -> geozero::error::Result<()> {
        let arr: MultiPolygonArray<i64> = vec![mp0(), mp1()].into();
        let wkt = arr.to_wkt()?;
        let expected = "GEOMETRYCOLLECTION(MULTIPOLYGON(((-111 45,-111 41,-104 41,-104 45,-111 45)),((-111 45,-111 41,-104 41,-104 45,-111 45),(-110 44,-110 42,-105 42,-105 44,-110 44))),MULTIPOLYGON(((-111 45,-111 41,-104 41,-104 45,-111 45)),((-110 44,-110 42,-105 42,-105 44,-110 44))))";
        assert_eq!(wkt, expected);
        Ok(())
    }

    #[test]
    fn from_geozero() -> Result<()> {
        let geo = Geometry::GeometryCollection(
            vec![mp0(), mp1()]
                .into_iter()
                .map(Geometry::MultiPolygon)
                .collect(),
        );
        let multi_point_array: MultiPolygonArray<i32> = geo.to_line_string_array().unwrap();
        assert_eq!(multi_point_array.value_as_geo(0), mp0());
        assert_eq!(multi_point_array.value_as_geo(1), mp1());
        Ok(())
    }
}
