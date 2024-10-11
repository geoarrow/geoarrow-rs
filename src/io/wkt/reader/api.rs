use std::str::FromStr;

use ::wkt::Wkt;
use arrow_array::OffsetSizeTrait;

use crate::array::*;
use crate::error::{GeoArrowError, Result};
use crate::ArrayBase;

pub trait FromWKT: Sized {
    type Input<O: OffsetSizeTrait>;

    fn from_wkt<O: OffsetSizeTrait>(arr: &Self::Input<O>, coord_type: CoordType) -> Result<Self>;
}

// impl<const D: usize> FromWKT for PointArray<D> {
//     type Input<O: OffsetSizeTrait> = WKTArray<O>;

//     fn from_wkt<O: OffsetSizeTrait>(arr: &WKTArray<O>, coord_type: CoordType) -> Result<Self> {
//         let mut builder = PointBuilder::<D>::new_with_options(coord_type, arr.metadata().clone());
//         for row in arr.array.iter() {
//             if let Some(s) = row {
//                 let geom = Wkt::<f64>::from_str(s)
//                     .map_err(|err| GeoArrowError::WktError(err.to_string()))?;
//                 builder.push_geometry(Some(&geom))?;
//             } else {
//                 builder.push_null();
//             }
//         }

//         Ok(builder.finish())
//     }
// }

macro_rules! impl_from_wkt {
    ($array:ty, $builder:ty) => {
        impl<const D: usize> FromWKT for $array {
            type Input<O: OffsetSizeTrait> = WKTArray<O>;

            fn from_wkt<O: OffsetSizeTrait>(
                arr: &WKTArray<O>,
                coord_type: CoordType,
            ) -> Result<Self> {
                let mut builder = <$builder>::new_with_options(coord_type, arr.metadata().clone());
                for row in arr.array.iter() {
                    if let Some(s) = row {
                        let geom = Wkt::<f64>::from_str(s)
                            .map_err(|err| GeoArrowError::WktError(err.to_string()))?;
                        builder.push_geometry(Some(&geom))?;
                    } else {
                        builder.push_null();
                    }
                }

                Ok(builder.finish())
            }
        }
    };
}

impl_from_wkt!(PointArray<D>, PointBuilder<D>);
impl_from_wkt!(LineStringArray<D>, LineStringBuilder<D>);
impl_from_wkt!(PolygonArray<D>, PolygonBuilder<D>);
impl_from_wkt!(MultiPointArray<D>, MultiPointBuilder<D>);
impl_from_wkt!(MultiLineStringArray<D>, MultiLineStringBuilder<D>);
impl_from_wkt!(MultiPolygonArray<D>, MultiPolygonBuilder<D>);
