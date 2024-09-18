use std::fmt::Display;
use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_buffer::NullBuffer;
use arrow_schema::{DataType, FieldRef};

use crate::array::metadata::ArrayMetadata;
use crate::array::CoordType;
use crate::datatypes::GeoDataType;
use crate::trait_::NativeArrayRef;
use crate::NativeArray;

#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct GeometryArrayDyn(pub(crate) Arc<dyn NativeArray>);

impl GeometryArrayDyn {
    pub fn new(array: Arc<dyn NativeArray>) -> Self {
        Self(array)
    }

    pub fn inner(&self) -> &NativeArrayRef {
        &self.0
    }

    pub fn into_inner(self) -> NativeArrayRef {
        self.0
    }
}

impl From<NativeArrayRef> for GeometryArrayDyn {
    fn from(value: NativeArrayRef) -> Self {
        Self(value)
    }
}

impl From<GeometryArrayDyn> for NativeArrayRef {
    fn from(value: GeometryArrayDyn) -> Self {
        value.0
    }
}

impl NativeArray for GeometryArrayDyn {
    fn as_any(&self) -> &dyn std::any::Any {
        self.0.as_any()
    }

    fn data_type(&self) -> GeoDataType {
        self.0.data_type()
    }

    fn storage_type(&self) -> DataType {
        self.0.storage_type()
    }

    fn extension_field(&self) -> FieldRef {
        self.0.extension_field()
    }

    fn extension_name(&self) -> &str {
        self.0.extension_name()
    }

    fn into_array_ref(self) -> ArrayRef {
        todo!()
        // self.0.as_ref().clone().into_array_ref()
    }

    fn to_array_ref(&self) -> ArrayRef {
        self.0.to_array_ref()
    }

    fn coord_type(&self) -> CoordType {
        self.0.coord_type()
    }

    fn to_coord_type(&self, coord_type: CoordType) -> Arc<dyn NativeArray> {
        self.0.to_coord_type(coord_type)
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    fn nulls(&self) -> Option<&NullBuffer> {
        self.0.nulls()
    }

    fn metadata(&self) -> Arc<ArrayMetadata> {
        self.0.metadata()
    }

    fn with_metadata(&self, metadata: Arc<ArrayMetadata>) -> NativeArrayRef {
        self.0.with_metadata(metadata)
    }

    fn as_ref(&self) -> &dyn NativeArray {
        self.0.as_ref()
    }

    fn slice(&self, offset: usize, length: usize) -> Arc<dyn NativeArray> {
        self.0.slice(offset, length)
    }

    fn owned_slice(&self, offset: usize, length: usize) -> Arc<dyn NativeArray> {
        self.0.owned_slice(offset, length)
    }
}

impl Display for GeometryArrayDyn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "GeometryArrayDyn")
    }
}
