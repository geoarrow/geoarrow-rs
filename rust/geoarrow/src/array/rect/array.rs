use std::sync::Arc;

use arrow::array::AsArray;
use arrow::datatypes::Float64Type;
use arrow_array::{Array, ArrayRef, Float64Array, StructArray};
use arrow_buffer::NullBuffer;
use arrow_schema::{DataType, Field};

use crate::array::metadata::ArrayMetadata;
use crate::array::rect::RectBuilder;
use crate::array::{CoordBuffer, CoordType, SeparatedCoordBuffer};
use crate::datatypes::{rect_fields, Dimension, NativeType};
use crate::error::GeoArrowError;
use crate::scalar::Rect;
use crate::trait_::{ArrayAccessor, GeometryArraySelfMethods, IntoArrow};
use crate::util::owned_slice_validity;
use crate::{ArrayBase, NativeArray};
use geo_traits::RectTrait;

/// An immutable array of Rect geometries.
///
/// This is **not** an array type defined by the GeoArrow specification (as of spec version 0.1)
/// but is included here for parity with georust/geo, and to save memory for the output of
/// `bounds()`.
///
/// Internally this is implemented as a FixedSizeList, laid out as minx, miny, maxx, maxy.
#[derive(Debug, Clone, PartialEq)]
pub struct RectArray {
    // Always NativeType::Rect
    data_type: NativeType,

    metadata: Arc<ArrayMetadata>,

    /// Separated arrays for each of the "lower" dimensions
    lower: SeparatedCoordBuffer,

    /// Separated arrays for each of the "upper" dimensions
    upper: SeparatedCoordBuffer,

    validity: Option<NullBuffer>,
}

impl RectArray {
    pub fn new(
        lower: SeparatedCoordBuffer,
        upper: SeparatedCoordBuffer,
        validity: Option<NullBuffer>,
        metadata: Arc<ArrayMetadata>,
    ) -> Self {
        assert_eq!(lower.dim(), upper.dim());
        let data_type = NativeType::Rect(lower.dim());
        Self {
            data_type,
            lower,
            upper,
            validity,
            metadata,
        }
    }

    pub fn lower(&self) -> &SeparatedCoordBuffer {
        &self.lower
    }

    pub fn upper(&self) -> &SeparatedCoordBuffer {
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

impl ArrayBase for RectArray {
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

impl NativeArray for RectArray {
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

impl GeometryArraySelfMethods for RectArray {
    fn with_coords(self, _coords: CoordBuffer) -> Self {
        unimplemented!()
    }

    fn into_coord_type(self, _coord_type: CoordType) -> Self {
        unimplemented!()
    }
}

impl<'a> ArrayAccessor<'a> for RectArray {
    type Item = Rect<'a>;
    type ItemGeo = geo::Rect;

    unsafe fn value_unchecked(&'a self, index: usize) -> Self::Item {
        Rect::new(&self.lower, &self.upper, index)
    }
}

impl IntoArrow for RectArray {
    type ArrowArray = StructArray;

    fn into_arrow(self) -> Self::ArrowArray {
        let fields = rect_fields(self.data_type.dimension().unwrap());
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

impl TryFrom<(&StructArray, Dimension)> for RectArray {
    type Error = GeoArrowError;

    fn try_from((value, dim): (&StructArray, Dimension)) -> Result<Self, Self::Error> {
        let validity = value.nulls();
        let columns = value.columns();
        assert_eq!(columns.len(), dim.size() * 2);

        let lower = match dim {
            Dimension::XY => {
                core::array::from_fn(|i| columns[i].as_primitive::<Float64Type>().values().clone())
            }
            Dimension::XYZ => {
                core::array::from_fn(|i| columns[i].as_primitive::<Float64Type>().values().clone())
            }
        };
        let upper = match dim {
            Dimension::XY => {
                core::array::from_fn(|i| columns[i].as_primitive::<Float64Type>().values().clone())
            }
            Dimension::XYZ => {
                core::array::from_fn(|i| columns[i].as_primitive::<Float64Type>().values().clone())
            }
        };

        Ok(Self::new(
            SeparatedCoordBuffer::new(lower, dim),
            SeparatedCoordBuffer::new(upper, dim),
            validity.cloned(),
            Default::default(),
        ))
    }
}

impl TryFrom<(&dyn Array, Dimension)> for RectArray {
    type Error = GeoArrowError;

    fn try_from((value, dim): (&dyn Array, Dimension)) -> Result<Self, Self::Error> {
        match value.data_type() {
            DataType::Struct(_) => {
                let arr = value.as_any().downcast_ref::<StructArray>().unwrap();
                (arr, dim).try_into()
            }
            _ => Err(GeoArrowError::General(
                "Invalid data type for RectArray".to_string(),
            )),
        }
    }
}

impl TryFrom<(&dyn Array, &Field)> for RectArray {
    type Error = GeoArrowError;

    fn try_from((arr, field): (&dyn Array, &Field)) -> Result<Self, Self::Error> {
        let geom_type = NativeType::try_from(field)?;
        let dim = geom_type
            .dimension()
            .ok_or(GeoArrowError::General("Expected dimension".to_string()))?;
        let mut arr: Self = (arr, dim).try_into()?;
        arr.metadata = Arc::new(ArrayMetadata::try_from(field)?);
        Ok(arr)
    }
}

impl<G: RectTrait<T = f64>> From<(&[G], Dimension)> for RectArray {
    fn from(other: (&[G], Dimension)) -> Self {
        let mut_arr: RectBuilder = other.into();
        mut_arr.into()
    }
}

impl<G: RectTrait<T = f64>> From<(Vec<Option<G>>, Dimension)> for RectArray {
    fn from(other: (Vec<Option<G>>, Dimension)) -> Self {
        let mut_arr: RectBuilder = other.into();
        mut_arr.into()
    }
}
