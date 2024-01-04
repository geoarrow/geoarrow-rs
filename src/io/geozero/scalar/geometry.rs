use crate::geo_traits::{GeometryTrait, GeometryType};
use crate::io::geozero::scalar::geometry_collection::process_geometry_collection;
use crate::io::geozero::scalar::linestring::process_line_string;
use crate::io::geozero::scalar::multilinestring::process_multi_line_string;
use crate::io::geozero::scalar::multipoint::process_multi_point;
use crate::io::geozero::scalar::multipolygon::process_multi_polygon;
use crate::io::geozero::scalar::point::process_point;
use crate::io::geozero::scalar::polygon::process_polygon;
use crate::scalar::Geometry;
use arrow_array::OffsetSizeTrait;
use geozero::{GeomProcessor, GeozeroGeometry};

pub(crate) fn process_geometry<P: GeomProcessor>(
    geom: &impl GeometryTrait<T = f64>,
    geom_idx: usize,
    processor: &mut P,
) -> geozero::error::Result<()> {
    match geom.as_type() {
        GeometryType::Point(g) => process_point(g, geom_idx, processor)?,
        GeometryType::LineString(g) => process_line_string(g, geom_idx, processor)?,
        GeometryType::Polygon(g) => process_polygon(g, true, geom_idx, processor)?,
        GeometryType::MultiPoint(g) => process_multi_point(g, geom_idx, processor)?,
        GeometryType::MultiLineString(g) => process_multi_line_string(g, geom_idx, processor)?,
        GeometryType::MultiPolygon(g) => process_multi_polygon(g, geom_idx, processor)?,
        GeometryType::GeometryCollection(g) => process_geometry_collection(g, geom_idx, processor)?,
        GeometryType::Rect(_g) => todo!(),
    };

    Ok(())
}

impl<O: OffsetSizeTrait> GeozeroGeometry for Geometry<'_, O> {
    fn process_geom<P: GeomProcessor>(&self, processor: &mut P) -> geozero::error::Result<()>
    where
        Self: Sized,
    {
        process_geometry(&self, 0, processor)
    }
}
