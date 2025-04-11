use geo_traits::{GeometryTrait, GeometryType};
use geozero::{GeomProcessor, GeozeroGeometry};

use super::{
    process_geometry_collection, process_line_string, process_multi_line_string,
    process_multi_point, process_multi_polygon, process_point, process_polygon,
};
use crate::scalar::Geometry;

pub(crate) fn process_geometry<P: GeomProcessor>(
    geom: &impl GeometryTrait<T = f64>,
    geom_idx: usize,
    processor: &mut P,
) -> geozero::error::Result<()> {
    use GeometryType::*;

    match geom.as_type() {
        Point(g) => process_point(g, geom_idx, processor)?,
        LineString(g) => process_line_string(g, geom_idx, processor)?,
        Polygon(g) => process_polygon(g, true, geom_idx, processor)?,
        MultiPoint(g) => process_multi_point(g, geom_idx, processor)?,
        MultiLineString(g) => process_multi_line_string(g, geom_idx, processor)?,
        MultiPolygon(g) => process_multi_polygon(g, geom_idx, processor)?,
        GeometryCollection(g) => process_geometry_collection(g, geom_idx, processor)?,
        Rect(_g) => todo!(),
        Triangle(_) | Line(_) => todo!(),
    };

    Ok(())
}

impl GeozeroGeometry for Geometry<'_> {
    fn process_geom<P: GeomProcessor>(&self, processor: &mut P) -> geozero::error::Result<()>
    where
        Self: Sized,
    {
        process_geometry(&self, 0, processor)
    }
}
