use std::io::Cursor;

use arrow_array::OffsetSizeTrait;
use geozero::wkb::process_wkb_geom;
use geozero::{GeomProcessor, GeozeroGeometry};

use crate::scalar::WKB;

impl<O: OffsetSizeTrait> GeozeroGeometry for WKB<O> {
    fn process_geom<P: GeomProcessor>(&self, processor: &mut P) -> geozero::error::Result<()>
    where
        Self: Sized,
    {
        process_wkb_geom(&mut Cursor::new(self.as_ref()), processor)
    }
}
