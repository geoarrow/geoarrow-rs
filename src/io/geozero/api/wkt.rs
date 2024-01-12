use std::sync::Arc;

use crate::algorithm::native::Downcast;
use crate::array::geometrycollection::GeometryCollectionBuilder;
use crate::array::metadata::ArrayMetadata;
use crate::array::*;
use crate::chunked_array::{
    ChunkedArray, ChunkedGeometryArrayTrait, ChunkedGeometryCollectionArray,
    ChunkedMixedGeometryArray,
};
use crate::error::Result;
use crate::io::geozero::array::MixedGeometryStreamBuilder;
use crate::GeometryArrayTrait;
use arrow_array::{Array, GenericStringArray, OffsetSizeTrait};
use geozero::{GeozeroGeometry, ToGeo};

pub trait FromWKT: Sized {
    type Input<O: OffsetSizeTrait>;

    fn from_wkt<O: OffsetSizeTrait>(
        arr: &Self::Input<O>,
        coord_type: CoordType,
        metadata: Arc<ArrayMetadata>,
        prefer_multi: bool,
    ) -> Result<Self>;
}

impl<OOutput: OffsetSizeTrait> FromWKT for MixedGeometryArray<OOutput> {
    type Input<O: OffsetSizeTrait> = GenericStringArray<O>;

    fn from_wkt<O: OffsetSizeTrait>(
        arr: &Self::Input<O>,
        coord_type: CoordType,
        metadata: Arc<ArrayMetadata>,
        prefer_multi: bool,
    ) -> Result<Self> {
        let mut builder =
            MixedGeometryStreamBuilder::new_with_options(coord_type, metadata, prefer_multi);
        for i in 0..arr.len() {
            if arr.is_valid(i) {
                let wkt_str = geozero::wkt::WktStr(arr.value(i));
                wkt_str.process_geom(&mut builder)?;
            } else {
                builder.push_null();
            }
        }

        Ok(builder.finish())
    }
}

impl<OOutput: OffsetSizeTrait> FromWKT for GeometryCollectionArray<OOutput> {
    type Input<O: OffsetSizeTrait> = GenericStringArray<O>;

    fn from_wkt<O: OffsetSizeTrait>(
        arr: &Self::Input<O>,
        coord_type: CoordType,
        metadata: Arc<ArrayMetadata>,
        prefer_multi: bool,
    ) -> Result<Self> {
        // TODO: Add GeometryCollectionStreamBuilder and use that instead of going through geo
        let mut builder = GeometryCollectionBuilder::new_with_options(coord_type, metadata);
        for i in 0..arr.len() {
            if arr.is_valid(i) {
                let wkt_str = geozero::wkt::WktStr(arr.value(i));
                let geo_geom = wkt_str.to_geo()?;
                builder.push_geometry(Some(&geo_geom), prefer_multi)?;
            } else {
                builder.push_null();
            }
        }

        Ok(builder.finish())
    }
}

impl FromWKT for Arc<dyn GeometryArrayTrait> {
    type Input<O: OffsetSizeTrait> = GenericStringArray<O>;

    fn from_wkt<O: OffsetSizeTrait>(
        arr: &Self::Input<O>,
        coord_type: CoordType,
        metadata: Arc<ArrayMetadata>,
        prefer_multi: bool,
    ) -> Result<Self> {
        let geom_arr =
            GeometryCollectionArray::<i64>::from_wkt(arr, coord_type, metadata, prefer_multi)?;
        Ok(geom_arr.downcast(true))
    }
}

impl<OOutput: OffsetSizeTrait> FromWKT for ChunkedMixedGeometryArray<OOutput> {
    type Input<O: OffsetSizeTrait> = ChunkedArray<GenericStringArray<O>>;

    fn from_wkt<O: OffsetSizeTrait>(
        arr: &Self::Input<O>,
        coord_type: CoordType,
        metadata: Arc<ArrayMetadata>,
        prefer_multi: bool,
    ) -> Result<Self> {
        arr.try_map(|chunk| FromWKT::from_wkt(chunk, coord_type, metadata.clone(), prefer_multi))?
            .try_into()
    }
}

impl<OOutput: OffsetSizeTrait> FromWKT for ChunkedGeometryCollectionArray<OOutput> {
    type Input<O: OffsetSizeTrait> = ChunkedArray<GenericStringArray<O>>;

    fn from_wkt<O: OffsetSizeTrait>(
        arr: &Self::Input<O>,
        coord_type: CoordType,
        metadata: Arc<ArrayMetadata>,
        prefer_multi: bool,
    ) -> Result<Self> {
        arr.try_map(|chunk| FromWKT::from_wkt(chunk, coord_type, metadata.clone(), prefer_multi))?
            .try_into()
    }
}

impl FromWKT for Arc<dyn ChunkedGeometryArrayTrait> {
    type Input<O: OffsetSizeTrait> = ChunkedArray<GenericStringArray<O>>;

    fn from_wkt<O: OffsetSizeTrait>(
        arr: &Self::Input<O>,
        coord_type: CoordType,
        metadata: Arc<ArrayMetadata>,
        prefer_multi: bool,
    ) -> Result<Self> {
        let geom_arr = ChunkedGeometryCollectionArray::<i64>::from_wkt(
            arr,
            coord_type,
            metadata,
            prefer_multi,
        )?;
        Ok(geom_arr.downcast(true))
    }
}

#[cfg(test)]
mod test {
    use crate::datatypes::GeoDataType;
    use crate::trait_::GeometryArrayAccessor;
    use arrow_array::builder::StringBuilder;

    use super::*;

    #[test]
    fn test_read_wkt() {
        let wkt_geoms = [
            "POINT (30 10)",
            "LINESTRING (30 10, 10 30, 40 40)",
            "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))",
        ];
        let mut builder = StringBuilder::new();
        wkt_geoms.iter().for_each(|s| builder.append_value(s));
        let arr = builder.finish();
        // dbg!(arr);
        let geom_arr = MixedGeometryArray::<i32>::from_wkt(
            &arr,
            Default::default(),
            Default::default(),
            false,
        )
        .unwrap();
        let geo_point = geo::Point::try_from(geom_arr.value(0).to_geo().unwrap()).unwrap();
        assert_eq!(geo_point.x(), 30.0);
        assert_eq!(geo_point.y(), 10.0);
    }

    #[test]
    fn test_read_wkt_downcast_from_multi() {
        let wkt_geoms = ["POINT (30 10)", "POINT (20 5)", "POINT (3 10)"];
        let mut builder = StringBuilder::new();
        wkt_geoms.iter().for_each(|s| builder.append_value(s));
        let arr = builder.finish();
        // dbg!(arr);
        let geom_arr =
            MixedGeometryArray::<i32>::from_wkt(&arr, Default::default(), Default::default(), true)
                .unwrap();
        let geom_arr = geom_arr.downcast(true);
        assert!(matches!(geom_arr.data_type(), GeoDataType::Point(_)));
    }
}
