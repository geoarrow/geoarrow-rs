use std::io::Cursor;
use std::sync::Arc;

use crate::algorithm::native::Downcast;
use crate::array::geometrycollection::GeometryCollectionBuilder;
use crate::array::metadata::ArrayMetadata;
use crate::array::*;
use crate::chunked_array::{
    ChunkedGeometryArrayTrait, ChunkedGeometryCollectionArray, ChunkedMixedGeometryArray,
    ChunkedWKBArray,
};
use crate::error::Result;
use crate::io::geozero::array::MixedGeometryStreamBuilder;
use crate::GeometryArrayTrait;
use arrow_array::{Array, OffsetSizeTrait};
use geozero::geo_types::GeoWriter;
use geozero::wkb::process_ewkb_geom;

pub trait FromEWKB: Sized {
    type Input<O: OffsetSizeTrait>;

    fn from_ewkb<O: OffsetSizeTrait>(
        arr: &Self::Input<O>,
        coord_type: CoordType,
        metadata: Arc<ArrayMetadata>,
        prefer_multi: bool,
    ) -> Result<Self>;
}

impl<OOutput: OffsetSizeTrait> FromEWKB for MixedGeometryArray<OOutput> {
    type Input<O: OffsetSizeTrait> = WKBArray<O>;

    fn from_ewkb<O: OffsetSizeTrait>(
        arr: &Self::Input<O>,
        coord_type: CoordType,
        metadata: Arc<ArrayMetadata>,
        prefer_multi: bool,
    ) -> Result<Self> {
        let arr = arr.clone().into_inner();
        let mut builder =
            MixedGeometryStreamBuilder::new_with_options(coord_type, metadata, prefer_multi);
        for i in 0..arr.len() {
            if arr.is_valid(i) {
                process_ewkb_geom(&mut Cursor::new(arr.value(i)), &mut builder)?;
            } else {
                builder.push_null();
            }
        }

        Ok(builder.finish())
    }
}

impl<OOutput: OffsetSizeTrait> FromEWKB for GeometryCollectionArray<OOutput> {
    type Input<O: OffsetSizeTrait> = WKBArray<O>;

    fn from_ewkb<O: OffsetSizeTrait>(
        arr: &Self::Input<O>,
        coord_type: CoordType,
        metadata: Arc<ArrayMetadata>,
        prefer_multi: bool,
    ) -> Result<Self> {
        // TODO: Add GeometryCollectionStreamBuilder and use that instead of going through geo
        let arr = arr.clone().into_inner();
        let mut builder = GeometryCollectionBuilder::new_with_options(coord_type, metadata);
        for i in 0..arr.len() {
            if arr.is_valid(i) {
                let buf = arr.value(i);
                let mut geo = GeoWriter::new();
                process_ewkb_geom(&mut Cursor::new(buf), &mut geo).unwrap();
                let geo_geom = geo.take_geometry().unwrap();
                builder.push_geometry(Some(&geo_geom), prefer_multi)?;
            } else {
                builder.push_null();
            }
        }

        Ok(builder.finish())
    }
}

impl FromEWKB for Arc<dyn GeometryArrayTrait> {
    type Input<O: OffsetSizeTrait> = WKBArray<O>;

    fn from_ewkb<O: OffsetSizeTrait>(
        arr: &Self::Input<O>,
        coord_type: CoordType,
        metadata: Arc<ArrayMetadata>,
        prefer_multi: bool,
    ) -> Result<Self> {
        let geom_arr =
            GeometryCollectionArray::<i64>::from_ewkb(arr, coord_type, metadata, prefer_multi)?;
        Ok(geom_arr.downcast(true))
    }
}

impl<OOutput: OffsetSizeTrait> FromEWKB for ChunkedMixedGeometryArray<OOutput> {
    type Input<O: OffsetSizeTrait> = ChunkedWKBArray<O>;

    fn from_ewkb<O: OffsetSizeTrait>(
        arr: &Self::Input<O>,
        coord_type: CoordType,
        metadata: Arc<ArrayMetadata>,
        prefer_multi: bool,
    ) -> Result<Self> {
        arr.try_map(|chunk| FromEWKB::from_ewkb(chunk, coord_type, metadata.clone(), prefer_multi))?
            .try_into()
    }
}

impl<OOutput: OffsetSizeTrait> FromEWKB for ChunkedGeometryCollectionArray<OOutput> {
    type Input<O: OffsetSizeTrait> = ChunkedWKBArray<O>;

    fn from_ewkb<O: OffsetSizeTrait>(
        arr: &Self::Input<O>,
        coord_type: CoordType,
        metadata: Arc<ArrayMetadata>,
        prefer_multi: bool,
    ) -> Result<Self> {
        arr.try_map(|chunk| FromEWKB::from_ewkb(chunk, coord_type, metadata.clone(), prefer_multi))?
            .try_into()
    }
}

impl FromEWKB for Arc<dyn ChunkedGeometryArrayTrait> {
    type Input<O: OffsetSizeTrait> = ChunkedWKBArray<O>;

    fn from_ewkb<O: OffsetSizeTrait>(
        arr: &Self::Input<O>,
        coord_type: CoordType,
        metadata: Arc<ArrayMetadata>,
        prefer_multi: bool,
    ) -> Result<Self> {
        let geom_arr = ChunkedGeometryCollectionArray::<i64>::from_ewkb(
            arr,
            coord_type,
            metadata,
            prefer_multi,
        )?;
        Ok(geom_arr.downcast(true))
    }
}
