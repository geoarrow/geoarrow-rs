use arrow::array::GenericBinaryBuilder;
use arrow_array::OffsetSizeTrait;
use geos::Geom;

use crate::array::WKBArray;
use crate::error::Result;

impl<O: OffsetSizeTrait> WKBArray<O> {
    pub fn from_geos(value: Vec<Option<geos::Geometry>>) -> Result<Self> {
        let mut builder = GenericBinaryBuilder::new();
        for maybe_geom in value {
            if let Some(geom) = maybe_geom {
                let buf = geom.to_wkb()?;
                builder.append_value(buf);
            } else {
                builder.append_null();
            }
        }

        Ok(WKBArray::new(builder.finish(), Default::default()))
    }
}
