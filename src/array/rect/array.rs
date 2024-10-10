use std::sync::Arc;

use arrow::array::AsArray;
use arrow::datatypes::Float64Type;
use arrow_array::{Array, ArrayRef, Float64Array, StructArray};
use arrow_buffer::NullBuffer;
use arrow_schema::{DataType, Field};

use crate::array::metadata::ArrayMetadata;
use crate::array::rect::RectBuilder;
use crate::array::{CoordBuffer, CoordType, SeparatedCoordBuffer};
use crate::datatypes::{rect_fields, NativeType};
use crate::error::GeoArrowError;
use crate::geo_traits::RectTrait;
use crate::scalar::{Geometry, Rect};
use crate::trait_::{ArrayAccessor, GeometryArraySelfMethods, IntoArrow, NativeGeometryAccessor};
use crate::util::owned_slice_validity;
use crate::{ArrayBase, NativeArray};

/// An immutable array of Rect geometries.
///
/// This is **not** an array type defined by the GeoArrow specification (as of spec version 0.1)
/// but is included here for parity with georust/geo, and to save memory for the output of
/// `bounds()`.
///
/// Internally this is implemented as a FixedSizeList, laid out as minx, miny, maxx, maxy.
#[derive(Debug, Clone, PartialEq)]
pub struct RectArray<const D: usize> {
    // Always NativeType::Rect
    data_type: NativeType,

    metadata: Arc<ArrayMetadata>,

    /// Separated arrays for each of the "lower" dimensions
    lower: SeparatedCoordBuffer<D>,

    /// Separated arrays for each of the "upper" dimensions
    upper: SeparatedCoordBuffer<D>,

    validity: Option<NullBuffer>,
}

impl<const D: usize> RectArray<D> {
    pub fn new(
        lower: SeparatedCoordBuffer<D>,
        upper: SeparatedCoordBuffer<D>,
        validity: Option<NullBuffer>,
        metadata: Arc<ArrayMetadata>,
    ) -> Self {
        let data_type = NativeType::Rect(D.try_into().unwrap());
        Self {
            data_type,
            lower,
            upper,
            validity,
            metadata,
        }
    }

    pub fn lower(&self) -> &SeparatedCoordBuffer<D> {
        &self.lower
    }

    pub fn upper(&self) -> &SeparatedCoordBuffer<D> {
        &self.upper
    }

    /// Slices this [`RectArray`] in place.
    /// # Panic
    /// This function panics iff `offset + length > self.len()`.
    #[inline]
    pub fn slice(&self, offset: usize, length: usize) -> Self {
        assert!(
            offset + length <= self.len(),
            "offset + length may not exceed length of array"
        );

        Self {
            data_type: self.data_type,
            lower: self.lower().slice(offset, length),
            upper: self.upper().slice(offset, length),
            validity: self.validity.as_ref().map(|v| v.slice(offset, length)),
            metadata: self.metadata(),
        }
    }

    pub fn owned_slice(&self, offset: usize, length: usize) -> Self {
        let lower = self.lower.owned_slice(offset, length);
        let upper = self.upper.owned_slice(offset, length);
        let validity = owned_slice_validity(self.nulls(), offset, length);
        Self::new(lower, upper, validity, self.metadata())
    }
}

impl<const D: usize> ArrayBase for RectArray<D> {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn storage_type(&self) -> DataType {
        self.data_type.to_data_type()
    }

    fn extension_field(&self) -> Arc<Field> {
        self.data_type
            .to_field_with_metadata("geometry", true, &self.metadata)
            .into()
    }

    fn extension_name(&self) -> &str {
        self.data_type.extension_name()
    }

    fn into_array_ref(self) -> Arc<dyn Array> {
        Arc::new(self.into_arrow())
    }

    fn to_array_ref(&self) -> arrow_array::ArrayRef {
        self.clone().into_array_ref()
    }

    fn metadata(&self) -> Arc<ArrayMetadata> {
        self.metadata.clone()
    }

    /// Returns the number of geometries in this array
    #[inline]
    fn len(&self) -> usize {
        self.lower.len()
    }

    /// Returns the optional validity.
    #[inline]
    fn nulls(&self) -> Option<&NullBuffer> {
        self.validity.as_ref()
    }
}

impl<const D: usize> NativeArray for RectArray<D> {
    fn data_type(&self) -> NativeType {
        self.data_type
    }

    fn coord_type(&self) -> CoordType {
        CoordType::Separated
    }

    fn to_coord_type(&self, _coord_type: CoordType) -> Arc<dyn NativeArray> {
        Arc::new(self.clone())
    }

    fn with_metadata(&self, metadata: Arc<ArrayMetadata>) -> crate::trait_::NativeArrayRef {
        let mut arr = self.clone();
        arr.metadata = metadata;
        Arc::new(arr)
    }

    fn as_ref(&self) -> &dyn NativeArray {
        self
    }

    fn slice(&self, offset: usize, length: usize) -> Arc<dyn NativeArray> {
        Arc::new(self.slice(offset, length))
    }

    fn owned_slice(&self, offset: usize, length: usize) -> Arc<dyn NativeArray> {
        Arc::new(self.owned_slice(offset, length))
    }
}

impl<const D: usize> GeometryArraySelfMethods<D> for RectArray<D> {
    fn with_coords(self, _coords: CoordBuffer<D>) -> Self {
        unimplemented!()
    }

    fn into_coord_type(self, _coord_type: CoordType) -> Self {
        unimplemented!()
    }
}

impl<'a, const D: usize> NativeGeometryAccessor<'a, D> for RectArray<D> {
    unsafe fn value_as_geometry_unchecked(
        &'a self,
        index: usize,
    ) -> crate::scalar::Geometry<'a, D> {
        Geometry::Rect(Rect::new(&self.lower, &self.upper, index))
    }
}

impl<'a, const D: usize> ArrayAccessor<'a> for RectArray<D> {
    type Item = Rect<'a, D>;
    type ItemGeo = geo::Rect;

    unsafe fn value_unchecked(&'a self, index: usize) -> Self::Item {
        Rect::new(&self.lower, &self.upper, index)
    }
}

impl<const D: usize> IntoArrow for RectArray<D> {
    type ArrowArray = StructArray;

    fn into_arrow(self) -> Self::ArrowArray {
        let fields = rect_fields(D.try_into().unwrap());
        let mut arrays: Vec<ArrayRef> = vec![];
        for buf in self.lower.buffers {
            arrays.push(Arc::new(Float64Array::new(buf, None)));
        }
        for buf in self.upper.buffers {
            arrays.push(Arc::new(Float64Array::new(buf, None)));
        }
        let validity = self.validity;

        StructArray::new(fields, arrays, validity)
    }
}

impl<const D: usize> TryFrom<&StructArray> for RectArray<D> {
    type Error = GeoArrowError;

    fn try_from(value: &StructArray) -> Result<Self, Self::Error> {
        let validity = value.nulls();
        let columns = value.columns();
        assert_eq!(columns.len(), D * 2);

        let lower = match D {
            2 => {
                core::array::from_fn(|i| columns[i].as_primitive::<Float64Type>().values().clone())
            }
            3 => {
                core::array::from_fn(|i| columns[i].as_primitive::<Float64Type>().values().clone())
            }
            _ => panic!("unexpected dim"),
        };
        let upper = match D {
            2 => {
                core::array::from_fn(|i| columns[i].as_primitive::<Float64Type>().values().clone())
            }
            3 => {
                core::array::from_fn(|i| columns[i].as_primitive::<Float64Type>().values().clone())
            }
            _ => panic!("unexpected dim"),
        };

        Ok(Self::new(
            SeparatedCoordBuffer::new(lower),
            SeparatedCoordBuffer::new(upper),
            validity.cloned(),
            Default::default(),
        ))
    }
}

impl<const D: usize> TryFrom<&dyn Array> for RectArray<D> {
    type Error = GeoArrowError;

    fn try_from(value: &dyn Array) -> Result<Self, Self::Error> {
        match value.data_type() {
            DataType::Struct(_) => {
                let arr = value.as_any().downcast_ref::<StructArray>().unwrap();
                arr.try_into()
            }
            _ => Err(GeoArrowError::General(
                "Invalid data type for RectArray".to_string(),
            )),
        }
    }
}

impl<const D: usize> TryFrom<(&dyn Array, &Field)> for RectArray<D> {
    type Error = GeoArrowError;

    fn try_from((arr, field): (&dyn Array, &Field)) -> Result<Self, Self::Error> {
        let mut arr: Self = arr.try_into()?;
        arr.metadata = Arc::new(ArrayMetadata::try_from(field)?);
        Ok(arr)
    }
}

impl<G: RectTrait<T = f64>, const D: usize> From<&[G]> for RectArray<D> {
    fn from(other: &[G]) -> Self {
        let mut_arr: RectBuilder<D> = other.into();
        mut_arr.into()
    }
}

impl<G: RectTrait<T = f64>, const D: usize> From<Vec<Option<G>>> for RectArray<D> {
    fn from(other: Vec<Option<G>>) -> Self {
        let mut_arr: RectBuilder<D> = other.into();
        mut_arr.into()
    }
}
