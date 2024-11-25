use std::sync::Arc;

use crate::array::metadata::ArrayMetadata;
use crate::array::MixedGeometryArray;
use crate::datatypes::NativeType;

pub struct UnknownGeometryArray {
    data_type: NativeType,

    pub(crate) metadata: Arc<ArrayMetadata>,

    xy: MixedGeometryArray,
    xyz: MixedGeometryArray,
}
