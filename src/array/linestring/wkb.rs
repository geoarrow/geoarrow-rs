use arrow2::offset::Offsets;
use arrow2::types::Offset;

use crate::array::{LineStringArray, MutableLineStringArray, WKBArray};
use crate::error::GeoArrowError;
use crate::GeometryArrayTrait;

// // fn first_pass_from_wkb()

// impl<O: Offset> TryFrom<WKBArray<O>> for MutableLineStringArray<O> {
//     type Error = GeoArrowError;

//     fn try_from(value: WKBArray<O>) -> Result<Self, Self::Error> {
//         let geom_offsets = Offsets::<O>::with_capacity(value.len());
//         let validity = value.validity().cloned();

//         value.iter().

//         MutableLineStringArray::try_new(coords, geom_offsets, validity)
//     }
// }
