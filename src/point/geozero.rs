use crate::PointArray;
use crate::{GeometryArrayTrait, MutablePointArray};
use geozero::{GeomProcessor, GeozeroGeometry};

impl GeozeroGeometry for PointArray {
    fn process_geom<P: GeomProcessor>(&self, processor: &mut P) -> geozero::error::Result<()>
    where
        Self: Sized,
    {
        let num_geometries = self.len();
        processor.geometrycollection_begin(num_geometries, 0)?;

        for idx in 0..num_geometries {
            processor.point_begin(idx)?;
            processor.xy(self.coords.get_x(idx), self.coords.get_y(idx), 0)?;
            processor.point_end(idx)?;
        }

        processor.geometrycollection_end(num_geometries)?;
        Ok(())
    }
}

/// Convert to GeoArrow PointArray
pub trait ToGeoArrowPoint {
    /// Convert to GeoArrow PointArray
    fn to_geoarrow(&self) -> geozero::error::Result<PointArray>;

    /// Convert to a GeoArrow MutablePointArray
    fn to_mutable_geoarrow(&self) -> geozero::error::Result<MutablePointArray>;
}

impl<T: GeozeroGeometry> ToGeoArrowPoint for T {
    fn to_geoarrow(&self) -> geozero::error::Result<PointArray> {
        Ok(self.to_mutable_geoarrow()?.into())
    }

    fn to_mutable_geoarrow(&self) -> geozero::error::Result<MutablePointArray> {
        let mut mutable_point_array = MutablePointArray::new();
        self.process_geom(&mut mutable_point_array)?;
        Ok(mutable_point_array)
    }
}

#[allow(unused_variables)]
impl GeomProcessor for MutablePointArray {
    fn xy(&mut self, x: f64, y: f64, _idx: usize) -> geozero::error::Result<()> {
        self.coords.push_coord(geo::Coord { x, y });
        Ok(())
    }

    fn geometrycollection_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        // self.x.reserve_exact(size);
        // self.y.reserve_exact(size);
        Ok(())
    }

    // Override all other trait _begin methods
    fn circularstring_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        Err(geozero::error::GeozeroError::Geometry(
            "Only point geometries allowed".to_string(),
        ))
    }

    fn compoundcurve_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        Err(geozero::error::GeozeroError::Geometry(
            "Only point geometries allowed".to_string(),
        ))
    }

    fn tin_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        Err(geozero::error::GeozeroError::Geometry(
            "Only point geometries allowed".to_string(),
        ))
    }

    fn polygon_begin(
        &mut self,
        tagged: bool,
        size: usize,
        idx: usize,
    ) -> geozero::error::Result<()> {
        Err(geozero::error::GeozeroError::Geometry(
            "Only point geometries allowed".to_string(),
        ))
    }

    fn triangle_begin(
        &mut self,
        tagged: bool,
        size: usize,
        idx: usize,
    ) -> geozero::error::Result<()> {
        Err(geozero::error::GeozeroError::Geometry(
            "Only point geometries allowed".to_string(),
        ))
    }

    fn linestring_begin(
        &mut self,
        tagged: bool,
        size: usize,
        idx: usize,
    ) -> geozero::error::Result<()> {
        Err(geozero::error::GeozeroError::Geometry(
            "Only point geometries allowed".to_string(),
        ))
    }

    fn multicurve_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        Err(geozero::error::GeozeroError::Geometry(
            "Only point geometries allowed".to_string(),
        ))
    }

    fn multipoint_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        Err(geozero::error::GeozeroError::Geometry(
            "Only point geometries allowed".to_string(),
        ))
    }

    fn curvepolygon_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        Err(geozero::error::GeozeroError::Geometry(
            "Only point geometries allowed".to_string(),
        ))
    }

    fn multipolygon_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        Err(geozero::error::GeozeroError::Geometry(
            "Only point geometries allowed".to_string(),
        ))
    }

    fn multisurface_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        Err(geozero::error::GeozeroError::Geometry(
            "Only point geometries allowed".to_string(),
        ))
    }

    fn multilinestring_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        Err(geozero::error::GeozeroError::Geometry(
            "Only point geometries allowed".to_string(),
        ))
    }

    fn polyhedralsurface_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        Err(geozero::error::GeozeroError::Geometry(
            "Only point geometries allowed".to_string(),
        ))
    }
}

#[cfg(test)]
mod test {
    use super::ToGeoArrowPoint;
    use crate::GeometryArrayTrait;
    use geo::{line_string, point, Geometry, GeometryCollection, LineString, Point};

    fn p0() -> Point {
        point!(
            x: 0., y: 1.
        )
    }

    fn p1() -> Point {
        point!(
            x: 1., y: 2.
        )
    }

    fn p2() -> Point {
        point!(
            x: 2., y: 3.
        )
    }

    fn ls0() -> LineString {
        line_string![
            (x: 0., y: 1.),
            (x: 1., y: 2.)
        ]
    }

    #[test]
    fn from_geozero() {
        let geo = Geometry::GeometryCollection(GeometryCollection(vec![
            Geometry::Point(p0()),
            Geometry::Point(p1()),
            Geometry::Point(p2()),
        ]));
        let point_array = geo.to_geoarrow().unwrap();
        assert_eq!(point_array.value_as_geo(0), p0());
        assert_eq!(point_array.value_as_geo(1), p1());
        assert_eq!(point_array.value_as_geo(2), p2());
    }

    #[test]
    fn from_geozero_error_multiple_geom_types() {
        let geo = Geometry::GeometryCollection(GeometryCollection(vec![
            Geometry::Point(p0()),
            Geometry::LineString(ls0()),
        ]));
        let err = geo.to_geoarrow().unwrap_err();
        assert!(matches!(err, geozero::error::GeozeroError::Geometry(..)));
    }
}
