use arrow::array::GenericBinaryBuilder;
use arrow_array::OffsetSizeTrait;
use geos::Geom;

use crate::array::WKBArray;
use crate::error::GeoArrowError;

impl<O: OffsetSizeTrait> TryFrom<Vec<Option<geos::Geometry>>> for WKBArray<O> {
    type Error = GeoArrowError;

    fn try_from(value: Vec<Option<geos::Geometry>>) -> std::result::Result<Self, Self::Error> {
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
