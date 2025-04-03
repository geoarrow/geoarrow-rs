use std::fmt::Display;
use std::sync::Arc;

use arrow_array::Array;
use arrow_array::ArrayRef;
use arrow_buffer::NullBuffer;
use arrow_schema::Field;
use arrow_schema::{DataType, FieldRef};
use geoarrow_schema::{CoordType, Metadata};

use crate::array::wkt::WKTArray;
use crate::array::*;
use crate::datatypes::{NativeType, SerializedType};
use crate::error::Result;
use crate::trait_::{NativeArrayRef, SerializedArray, SerializedArrayRef};
use crate::{ArrayBase, NativeArray};

/// A wrapper around a NativeArray of unknown type.
#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct NativeArrayDyn(Arc<dyn NativeArray>);

impl NativeArrayDyn {
    /// Construct a new [NativeArrayDyn]
    pub fn new(array: Arc<dyn NativeArray>) -> Self {
        Self(array)
    }

    /// Construct a new [NativeArrayDyn] from an Arrow [Array] and [Field].
    // TODO: add an option to parse a serialized array to a native array here.
    pub fn from_arrow_array(array: &dyn Array, field: &Field) -> Result<Self> {
        use NativeType::*;
        let geo_arr: Arc<dyn NativeArray> = match NativeType::try_from(field)? {
            Point(_) => Arc::new(PointArray::try_from((array, field))?),
            LineString(_) => Arc::new(LineStringArray::try_from((array, field))?),
            Polygon(_) => Arc::new(PolygonArray::try_from((array, field))?),
            MultiPoint(_) => Arc::new(MultiPointArray::try_from((array, field))?),
            MultiLineString(_) => Arc::new(MultiLineStringArray::try_from((array, field))?),
            MultiPolygon(_) => Arc::new(MultiPolygonArray::try_from((array, field))?),
            GeometryCollection(_) => Arc::new(GeometryCollectionArray::try_from((array, field))?),
            Rect(_) => Arc::new(RectArray::try_from((array, field))?),
            Geometry(_) => Arc::new(GeometryArray::try_from((array, field))?),
        };

        Ok(Self(geo_arr))
    }

    /// Access the underlying [`Arc<dyn NativeArray>`]
    pub fn inner(&self) -> &NativeArrayRef {
        &self.0
    }

    /// Consume self and access the underlying [`Arc<dyn NativeArray>`]
    pub fn into_inner(self) -> NativeArrayRef {
        self.0
    }
}

impl From<NativeArrayRef> for NativeArrayDyn {
    fn from(value: NativeArrayRef) -> Self {
        Self(value)
    }
}

impl From<NativeArrayDyn> for NativeArrayRef {
    fn from(value: NativeArrayDyn) -> Self {
        value.0
    }
}

impl ArrayBase for NativeArrayDyn {
    fn as_any(&self) -> &dyn std::any::Any {
        self.0.as_any()
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
        // We can't move out of the Arc
        self.to_array_ref()
    }

    fn to_array_ref(&self) -> ArrayRef {
        self.0.to_array_ref()
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    fn nulls(&self) -> Option<&NullBuffer> {
        self.0.nulls()
    }

    fn metadata(&self) -> Arc<Metadata> {
        self.0.metadata()
    }
}

impl NativeArray for NativeArrayDyn {
    fn data_type(&self) -> NativeType {
        self.0.data_type()
    }

    fn coord_type(&self) -> CoordType {
        self.0.coord_type()
    }

    fn to_coord_type(&self, coord_type: CoordType) -> Arc<dyn NativeArray> {
        self.0.to_coord_type(coord_type)
    }

    fn with_metadata(&self, metadata: Arc<Metadata>) -> NativeArrayRef {
        self.0.with_metadata(metadata)
    }

    fn as_ref(&self) -> &dyn NativeArray {
        self.0.as_ref()
    }

    fn slice(&self, offset: usize, length: usize) -> Arc<dyn NativeArray> {
        self.0.slice(offset, length)
    }
}

impl Display for NativeArrayDyn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "NativeArrayDyn")
    }
}

/// A wrapper around a SerializedArray of unknown type.
#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct SerializedArrayDyn(pub(crate) SerializedArrayRef);

impl SerializedArrayDyn {
    /// Construct a new [SerializedArrayDyn]
    pub fn new(array: SerializedArrayRef) -> Self {
        Self(array)
    }

    /// Construct a new [SerializedArrayDyn] from an Arrow [Array] and [Field].
    pub fn from_arrow_array(array: &dyn Array, field: &Field) -> Result<Self> {
        let data_type = SerializedType::try_from(field)?;

        let geo_arr: SerializedArrayRef = match data_type {
            SerializedType::WKB(_) => Arc::new(WKBArray::<i32>::try_from((array, field))?),
            SerializedType::LargeWKB(_) => Arc::new(WKBArray::<i64>::try_from((array, field))?),
            SerializedType::WKT(_) => Arc::new(WKTArray::<i32>::try_from((array, field))?),
            SerializedType::LargeWKT(_) => Arc::new(WKTArray::<i64>::try_from((array, field))?),
        };

        Ok(Self(geo_arr))
    }

    /// Access the underlying [`Arc<dyn SerializedArray>`]
    pub fn inner(&self) -> &SerializedArrayRef {
        &self.0
    }

    /// Consume self and access the underlying [`Arc<dyn SerializedArray>`]
    pub fn into_inner(self) -> SerializedArrayRef {
        self.0
    }
}

impl ArrayBase for SerializedArrayDyn {
    fn as_any(&self) -> &dyn std::any::Any {
        self.0.as_any()
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
        // We can't move out of the Arc
        self.to_array_ref()
    }

    fn to_array_ref(&self) -> ArrayRef {
        self.0.to_array_ref()
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    fn nulls(&self) -> Option<&NullBuffer> {
        self.0.nulls()
    }

    fn metadata(&self) -> Arc<Metadata> {
        self.0.metadata()
    }
}

impl SerializedArray for SerializedArrayDyn {
    fn data_type(&self) -> SerializedType {
        self.0.data_type()
    }

    fn as_ref(&self) -> &dyn SerializedArray {
        self.0.as_ref()
    }

    fn with_metadata(&self, metadata: Arc<Metadata>) -> Arc<dyn SerializedArray> {
        self.0.with_metadata(metadata)
    }
}
