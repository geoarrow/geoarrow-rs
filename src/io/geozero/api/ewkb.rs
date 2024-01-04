use std::sync::Arc;

use crate::algorithm::native::Downcast;
use crate::array::geometrycollection::GeometryCollectionBuilder;
use crate::array::*;
use crate::chunked_array::{
    ChunkedGeometryCollectionArray, ChunkedMixedGeometryArray, ChunkedWKBArray,
};
use crate::error::Result;
use crate::io::geozero::array::mixed::MixedGeometryStreamBuilder;
use crate::GeometryArrayTrait;
use arrow_array::{Array, OffsetSizeTrait};
use geozero::{GeozeroGeometry, ToGeo};

pub trait FromEWKB: Sized {
    type Input<O: OffsetSizeTrait>;

    fn from_ewkb<O: OffsetSizeTrait>(arr: &Self::Input<O>, coord_type: CoordType) -> Result<Self>;
}

impl<OOutput: OffsetSizeTrait> FromEWKB for MixedGeometryArray<OOutput> {
    type Input<O: OffsetSizeTrait> = WKBArray<O>;

    fn from_ewkb<O: OffsetSizeTrait>(arr: &Self::Input<O>, coord_type: CoordType) -> Result<Self> {
        let arr = arr.clone().into_inner();
        let mut builder = MixedGeometryStreamBuilder::new_with_options(coord_type);
        for i in 0..arr.len() {
            if arr.is_valid(i) {
                let ewkb = geozero::wkb::Ewkb(arr.value(i).to_vec());
                ewkb.process_geom(&mut builder)?;
            } else {
                builder.push_null();
            }
        }

        Ok(builder.finish())
    }
}

impl<OOutput: OffsetSizeTrait> FromEWKB for GeometryCollectionArray<OOutput> {
    type Input<O: OffsetSizeTrait> = WKBArray<O>;

    fn from_ewkb<O: OffsetSizeTrait>(arr: &Self::Input<O>, coord_type: CoordType) -> Result<Self> {
        // TODO: Add GeometryCollectionStreamBuilder and use that instead of going through geo
        let arr = arr.clone().into_inner();
        let mut builder = GeometryCollectionBuilder::new_with_options(coord_type);
        for i in 0..arr.len() {
            if arr.is_valid(i) {
                let geo_geom = geozero::wkb::Ewkb(arr.value(i).to_vec()).to_geo()?;
                builder.push_geometry(Some(&geo_geom), true)?;
            } else {
                builder.push_null();
            }
        }

        Ok(builder.finish())
    }
}

impl FromEWKB for Arc<dyn GeometryArrayTrait> {
    type Input<O: OffsetSizeTrait> = WKBArray<O>;

    fn from_ewkb<O: OffsetSizeTrait>(arr: &Self::Input<O>, coord_type: CoordType) -> Result<Self> {
        let geom_arr = GeometryCollectionArray::<i64>::from_ewkb(arr, coord_type)?;
        Ok(geom_arr.downcast(true))
    }
}

impl<OOutput: OffsetSizeTrait> FromEWKB for ChunkedMixedGeometryArray<OOutput> {
    type Input<O: OffsetSizeTrait> = ChunkedWKBArray<O>;

    fn from_ewkb<O: OffsetSizeTrait>(arr: &Self::Input<O>, coord_type: CoordType) -> Result<Self> {
        arr.try_map(|chunk| FromEWKB::from_ewkb(chunk, coord_type))?
            .try_into()
    }
}

impl<OOutput: OffsetSizeTrait> FromEWKB for ChunkedGeometryCollectionArray<OOutput> {
    type Input<O: OffsetSizeTrait> = ChunkedWKBArray<O>;

    fn from_ewkb<O: OffsetSizeTrait>(arr: &Self::Input<O>, coord_type: CoordType) -> Result<Self> {
        arr.try_map(|chunk| FromEWKB::from_ewkb(chunk, coord_type))?
            .try_into()
    }
}
