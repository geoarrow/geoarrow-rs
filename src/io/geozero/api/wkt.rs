use std::sync::Arc;

use crate::array::geometrycollection::GeometryCollectionBuilder;
use crate::array::*;
use crate::error::Result;
use crate::GeometryArrayTrait;
use arrow_array::{Array, GenericStringArray, OffsetSizeTrait};
use geozero::ToGeo;

pub trait FromWKT: Sized {
    type Input<O: OffsetSizeTrait>;

    fn from_wkt<O: OffsetSizeTrait>(arr: &Self::Input<O>, coord_type: CoordType) -> Result<Self>;
}

impl<OOutput: OffsetSizeTrait> FromWKT for MixedGeometryArray<OOutput> {
    type Input<O: OffsetSizeTrait> = GenericStringArray<O>;

    fn from_wkt<O: OffsetSizeTrait>(arr: &Self::Input<O>, coord_type: CoordType) -> Result<Self> {
        let mut builder = MixedGeometryBuilder::new_with_options(coord_type);
        for i in 0..arr.len() {
            if arr.is_valid(i) {
                let wkt_str = geozero::wkt::WktStr(arr.value(i));
                let geo_geom = wkt_str.to_geo()?;
                builder.push_geometry(Some(&geo_geom))?;
            } else {
                builder.push_null();
            }
        }

        Ok(builder.finish())
    }
}

impl<OOutput: OffsetSizeTrait> FromWKT for GeometryCollectionArray<OOutput> {
    type Input<O: OffsetSizeTrait> = GenericStringArray<O>;

    fn from_wkt<O: OffsetSizeTrait>(arr: &Self::Input<O>, coord_type: CoordType) -> Result<Self> {
        let mut builder = GeometryCollectionBuilder::new_with_options(coord_type);
        for i in 0..arr.len() {
            if arr.is_valid(i) {
                let wkt_str = geozero::wkt::WktStr(arr.value(i));
                let geo_geom = wkt_str.to_geo()?;
                builder.push_geometry(Some(&geo_geom), true)?;
            } else {
                builder.push_null();
            }
        }

        Ok(builder.finish())
    }
}

impl FromWKT for Arc<dyn GeometryArrayTrait> {
    type Input<O: OffsetSizeTrait> = GenericStringArray<O>;

    fn from_wkt<O: OffsetSizeTrait>(arr: &Self::Input<O>, coord_type: CoordType) -> Result<Self> {
        let geom_arr = GeometryCollectionArray::<i64>::from_wkt(arr, coord_type)?;
        Ok(geom_arr.downcast())
    }
}
