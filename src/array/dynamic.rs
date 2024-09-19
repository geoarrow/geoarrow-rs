use std::fmt::Display;
use std::sync::Arc;

use arrow_array::Array;
use arrow_array::ArrayRef;
use arrow_buffer::NullBuffer;
use arrow_schema::Field;
use arrow_schema::{DataType, FieldRef};

use crate::array::metadata::ArrayMetadata;
use crate::array::CoordType;
use crate::array::*;
use crate::datatypes::{Dimension, NativeType, SerializedType};
use crate::error::Result;
use crate::trait_::{NativeArrayRef, SerializedArray, SerializedArrayRef};
use crate::{ArrayBase, NativeArray};

#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct NativeArrayDyn(Arc<dyn NativeArray>);

impl NativeArrayDyn {
    pub fn new(array: Arc<dyn NativeArray>) -> Self {
        Self(array)
    }

    pub fn from_arrow_array(array: &dyn Array, field: &Field) -> Result<Self> {
        let data_type = NativeType::try_from(field)?;

        use Dimension::*;
        use NativeType::*;
        let geo_arr: Arc<dyn NativeArray> = match data_type {
            Point(_, dim) => match dim {
                XY => Arc::new(PointArray::<2>::try_from((array, field))?),
                XYZ => Arc::new(PointArray::<3>::try_from((array, field))?),
            },
            LineString(_, dim) => match dim {
                XY => Arc::new(LineStringArray::<i32, 2>::try_from((array, field))?),
                XYZ => Arc::new(LineStringArray::<i32, 3>::try_from((array, field))?),
            },
            LargeLineString(_, dim) => match dim {
                XY => Arc::new(LineStringArray::<i64, 2>::try_from((array, field))?),
                XYZ => Arc::new(LineStringArray::<i64, 3>::try_from((array, field))?),
            },
            Polygon(_, dim) => match dim {
                XY => Arc::new(PolygonArray::<i32, 2>::try_from((array, field))?),
                XYZ => Arc::new(PolygonArray::<i32, 3>::try_from((array, field))?),
            },
            LargePolygon(_, dim) => match dim {
                XY => Arc::new(PolygonArray::<i64, 2>::try_from((array, field))?),
                XYZ => Arc::new(PolygonArray::<i64, 3>::try_from((array, field))?),
            },
            MultiPoint(_, dim) => match dim {
                XY => Arc::new(MultiPointArray::<i32, 2>::try_from((array, field))?),
                XYZ => Arc::new(MultiPointArray::<i32, 3>::try_from((array, field))?),
            },
            LargeMultiPoint(_, dim) => match dim {
                XY => Arc::new(MultiPointArray::<i64, 2>::try_from((array, field))?),
                XYZ => Arc::new(MultiPointArray::<i64, 3>::try_from((array, field))?),
            },
            MultiLineString(_, dim) => match dim {
                XY => Arc::new(MultiLineStringArray::<i32, 2>::try_from((array, field))?),
                XYZ => Arc::new(MultiLineStringArray::<i32, 3>::try_from((array, field))?),
            },
            LargeMultiLineString(_, dim) => match dim {
                XY => Arc::new(MultiLineStringArray::<i64, 2>::try_from((array, field))?),
                XYZ => Arc::new(MultiLineStringArray::<i64, 3>::try_from((array, field))?),
            },
            MultiPolygon(_, dim) => match dim {
                XY => Arc::new(MultiPolygonArray::<i32, 2>::try_from((array, field))?),
                XYZ => Arc::new(MultiPolygonArray::<i32, 3>::try_from((array, field))?),
            },
            LargeMultiPolygon(_, dim) => match dim {
                XY => Arc::new(MultiPolygonArray::<i64, 2>::try_from((array, field))?),
                XYZ => Arc::new(MultiPolygonArray::<i64, 3>::try_from((array, field))?),
            },
            Mixed(_, dim) => match dim {
                XY => Arc::new(MixedGeometryArray::<i32, 2>::try_from((array, field))?),
                XYZ => Arc::new(MixedGeometryArray::<i32, 3>::try_from((array, field))?),
            },
            LargeMixed(_, dim) => match dim {
                XY => Arc::new(MixedGeometryArray::<i64, 2>::try_from((array, field))?),
                XYZ => Arc::new(MixedGeometryArray::<i64, 3>::try_from((array, field))?),
            },
            GeometryCollection(_, dim) => match dim {
                XY => Arc::new(GeometryCollectionArray::<i32, 2>::try_from((array, field))?),
                XYZ => Arc::new(GeometryCollectionArray::<i32, 3>::try_from((array, field))?),
            },
            LargeGeometryCollection(_, dim) => match dim {
                XY => Arc::new(GeometryCollectionArray::<i64, 2>::try_from((array, field))?),
                XYZ => Arc::new(GeometryCollectionArray::<i64, 3>::try_from((array, field))?),
            },
            // WKB => Arc::new(WKBArray::<i32>::try_from((array, field))?),
            // LargeWKB => Arc::new(WKBArray::<i64>::try_from((array, field))?),
            Rect(dim) => match dim {
                XY => Arc::new(RectArray::<2>::try_from((array, field))?),
                XYZ => Arc::new(RectArray::<3>::try_from((array, field))?),
            },
        };

        Ok(Self(geo_arr))
    }

    pub fn inner(&self) -> &NativeArrayRef {
        &self.0
    }

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
        todo!()
        // self.0.as_ref().clone().into_array_ref()
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

    fn metadata(&self) -> Arc<ArrayMetadata> {
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

impl Display for NativeArrayDyn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "NativeArrayDyn")
    }
}

#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct SerializedArrayDyn(pub(crate) SerializedArrayRef);

impl SerializedArrayDyn {
    pub fn new(array: SerializedArrayRef) -> Self {
        Self(array)
    }

    pub fn from_arrow_array(array: &dyn Array, field: &Field) -> Result<Self> {
        let data_type = SerializedType::try_from(field)?;

        let geo_arr: SerializedArrayRef = match data_type {
            SerializedType::WKB => Arc::new(WKBArray::<i32>::try_from((array, field))?),
            SerializedType::LargeWKB => Arc::new(WKBArray::<i64>::try_from((array, field))?),
        };

        Ok(Self(geo_arr))
    }

    pub fn inner(&self) -> &SerializedArrayRef {
        &self.0
    }

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
        todo!()
        // self.0.as_ref().clone().into_array_ref()
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

    fn metadata(&self) -> Arc<ArrayMetadata> {
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

    fn with_metadata(&self, metadata: Arc<ArrayMetadata>) -> Arc<dyn SerializedArray> {
        self.0.with_metadata(metadata)
    }
}
