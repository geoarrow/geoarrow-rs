use std::io::Cursor;

use arrow_array::OffsetSizeTrait;
use geozero::wkb::process_wkb_geom;
use geozero::{GeomProcessor, GeozeroGeometry};

use crate::scalar::WKB;

impl<O: OffsetSizeTrait> GeozeroGeometry for WKB<'_, O> {
    fn process_geom<P: GeomProcessor>(&self, processor: &mut P) -> geozero::error::Result<()>
    where
        Self: Sized,
    {
        let slice = self.arr.value(self.geom_index);
        process_wkb_geom(&mut Cursor::new(slice), processor)
    }
}
