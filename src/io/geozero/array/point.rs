use crate::array::{PointArray, PointBuilder};
use crate::io::geozero::scalar::process_point;
use crate::trait_::ArrayAccessor;
use crate::ArrayBase;
use geozero::{GeomProcessor, GeozeroGeometry};

impl<const D: usize> GeozeroGeometry for PointArray<D> {
    fn process_geom<P: GeomProcessor>(&self, processor: &mut P) -> geozero::error::Result<()>
    where
        Self: Sized,
    {
        let num_geometries = self.len();
        processor.geometrycollection_begin(num_geometries, 0)?;

        for idx in 0..num_geometries {
            process_point(&self.value(idx), idx, processor)?;
        }

        processor.geometrycollection_end(num_geometries)?;
        Ok(())
    }
}

/// GeoZero trait to convert to GeoArrow PointArray.
pub trait ToPointArray<const D: usize> {
    /// Convert to GeoArrow PointArray
    fn to_point_array(&self) -> geozero::error::Result<PointArray<D>>;

    /// Convert to a GeoArrow PointBuilder
    fn to_point_builder(&self) -> geozero::error::Result<PointBuilder<D>>;
}

impl<T: GeozeroGeometry, const D: usize> ToPointArray<D> for T {
    fn to_point_array(&self) -> geozero::error::Result<PointArray<D>> {
        Ok(self.to_point_builder()?.into())
    }

    fn to_point_builder(&self) -> geozero::error::Result<PointBuilder<D>> {
        let mut mutable_point_array = PointBuilder::new();
        self.process_geom(&mut mutable_point_array)?;
        Ok(mutable_point_array)
    }
}

#[allow(unused_variables)]
impl<const D: usize> GeomProcessor for PointBuilder<D> {
    fn empty_point(&mut self, idx: usize) -> geozero::error::Result<()> {
        self.push_empty();
        Ok(())
    }

    fn xy(&mut self, x: f64, y: f64, _idx: usize) -> geozero::error::Result<()> {
        if x.is_finite() && y.is_finite() {
            self.push_point(Some(&geo::Point::new(x, y)));
        } else {
            self.push_null()
        }

        Ok(())
    }

    fn geometrycollection_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        self.reserve_exact(size);
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

    // fn multicurve_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
    //     Err(geozero::error::GeozeroError::Geometry(
    //         "Only point geometries allowed".to_string(),
    //     ))
    // }

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
    use super::*;
    use crate::array::PointArray;
    use crate::trait_::ArrayAccessor;
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
        let geo = Geometry::GeometryCollection(
            vec![
                Geometry::Point(p0()),
                Geometry::Point(p1()),
                Geometry::Point(p2()),
            ]
            .into(),
        );
        let point_array: PointArray<2> = geo.to_point_array().unwrap();
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
        let err = ToPointArray::<2>::to_point_array(&geo).unwrap_err();
        assert!(matches!(err, geozero::error::GeozeroError::Geometry(..)));
    }
}
