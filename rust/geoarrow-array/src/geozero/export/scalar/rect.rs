use geo_traits::RectTrait;
use geozero::{GeomProcessor, GeozeroGeometry};

// use crate::builder::geo_trait_wrappers::RectWrapper;
use crate::geozero::export::scalar::process_polygon;
use crate::scalar::Rect;

pub(crate) fn process_rect<P: GeomProcessor>(
    geom: &impl RectTrait<T = f64>,
    geom_idx: usize,
    processor: &mut P,
) -> geozero::error::Result<()> {
    todo!()
    // let polygon = RectWrapper::try_new(geom)
    //     .map_err(|err| geozero::error::GeozeroError::Geometry(err.to_string()))?;
    // process_polygon(&polygon, true, geom_idx, processor)
}

impl GeozeroGeometry for Rect<'_> {
    fn process_geom<P: GeomProcessor>(&self, processor: &mut P) -> geozero::error::Result<()>
    where
        Self: Sized,
    {
        process_rect(self, 0, processor)
    }
}
