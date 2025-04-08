use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Float64Type;
use arrow_array::{Array, ArrayRef, StructArray};
use arrow_buffer::{NullBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field};
use geoarrow_schema::{BoxType, Metadata};

use crate::array::SeparatedCoordBuffer;
use crate::datatypes::GeoArrowType;
use crate::error::{GeoArrowError, Result};
use crate::scalar::Rect;
use crate::trait_::{ArrayAccessor, GeoArrowArray, IntoArrow};

/// An immutable array of Rect or Box geometries.
///
/// A rect is an axis-aligned bounded rectangle whose area is defined by minimum and maximum
/// coordinates.
///
/// All rects must have the same dimension.
///
/// This is **not** an array type defined by the GeoArrow specification (as of spec version 0.1)
/// but is included here for parity with georust/geo, and to save memory for the output of
/// `bounds()`.
///
/// Internally this is implemented as a FixedSizeList, laid out as minx, miny, maxx, maxy.
#[derive(Debug, Clone, PartialEq)]
pub struct RectArray {
    pub(crate) data_type: BoxType,

    /// Separated arrays for each of the "lower" dimensions
    lower: SeparatedCoordBuffer,

    /// Separated arrays for each of the "upper" dimensions
    upper: SeparatedCoordBuffer,

    validity: Option<NullBuffer>,
}

impl RectArray {
    /// Construct a new [`RectArray`] from parts
    pub fn new(
        lower: SeparatedCoordBuffer,
        upper: SeparatedCoordBuffer,
        validity: Option<NullBuffer>,
        metadata: Arc<Metadata>,
    ) -> Self {
        assert_eq!(lower.dim(), upper.dim());
        Self {
            data_type: BoxType::new(lower.dim(), metadata),
            lower,
            upper,
            validity,
        }
    }

    /// Access the coordinate buffer of the "lower" corner of the RectArray
    ///
    /// Note that this needs to be interpreted in conjunction with the [null buffer][Self::nulls].
    pub fn lower(&self) -> &SeparatedCoordBuffer {
        &self.lower
    }

    /// Access the coordinate buffer of the "upper" corner of the RectArray
    ///
    /// Note that this needs to be interpreted in conjunction with the [null buffer][Self::nulls].
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
            data_type: self.data_type.clone(),
            lower: self.lower().slice(offset, length),
            upper: self.upper().slice(offset, length),
            validity: self.validity.as_ref().map(|v| v.slice(offset, length)),
        }
    }
}

impl GeoArrowArray for RectArray {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn into_array_ref(self) -> ArrayRef {
        Arc::new(self.into_arrow())
    }

    fn to_array_ref(&self) -> ArrayRef {
        self.clone().into_array_ref()
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

    fn data_type(&self) -> GeoArrowType {
        GeoArrowType::Rect(self.data_type.clone())
    }

    fn slice(&self, offset: usize, length: usize) -> Arc<dyn GeoArrowArray> {
        Arc::new(self.slice(offset, length))
    }
}

impl<'a> ArrayAccessor<'a> for RectArray {
    type Item = Rect<'a>;

    unsafe fn value_unchecked(&'a self, index: usize) -> Result<Self::Item> {
        Ok(Rect::new(&self.lower, &self.upper, index))
    }
}

impl IntoArrow for RectArray {
    type ArrowArray = StructArray;
    type ExtensionType = BoxType;

    fn into_arrow(self) -> Self::ArrowArray {
        let fields = match self.data_type.data_type() {
            DataType::Struct(fields) => fields,
            _ => unreachable!(),
        };

        let mut arrays: Vec<ArrayRef> = vec![];

        // values_array takes care of the correct number of dimensions
        arrays.extend_from_slice(self.lower.values_array().as_slice());
        arrays.extend_from_slice(self.upper.values_array().as_slice());

        let validity = self.validity;
        StructArray::new(fields, arrays, validity)
    }

    fn ext_type(&self) -> &Self::ExtensionType {
        &self.data_type
    }
}

impl TryFrom<(&StructArray, BoxType)> for RectArray {
    type Error = GeoArrowError;

    fn try_from((value, typ): (&StructArray, BoxType)) -> Result<Self> {
        let dim = typ.dimension();
        let validity = value.nulls();
        let columns = value.columns();
        assert_eq!(columns.len(), dim.size() * 2);

        let dim_size = dim.size();
        let lower = core::array::from_fn(|i| {
            if i < dim_size {
                columns[i].as_primitive::<Float64Type>().values().clone()
            } else {
                ScalarBuffer::from(vec![])
            }
        });
        let upper = core::array::from_fn(|i| {
            if i < dim_size {
                columns[dim_size + i]
                    .as_primitive::<Float64Type>()
                    .values()
                    .clone()
            } else {
                ScalarBuffer::from(vec![])
            }
        });
        Ok(Self::new(
            SeparatedCoordBuffer::new(lower, dim),
            SeparatedCoordBuffer::new(upper, dim),
            validity.cloned(),
            typ.metadata().clone(),
        ))
    }
}

impl TryFrom<(&dyn Array, BoxType)> for RectArray {
    type Error = GeoArrowError;

    fn try_from((value, dim): (&dyn Array, BoxType)) -> Result<Self> {
        match value.data_type() {
            DataType::Struct(_) => (value.as_struct(), dim).try_into(),
            _ => Err(GeoArrowError::General(
                "Invalid data type for RectArray".to_string(),
            )),
        }
    }
}

impl TryFrom<(&dyn Array, &Field)> for RectArray {
    type Error = GeoArrowError;

    fn try_from((arr, field): (&dyn Array, &Field)) -> Result<Self> {
        let typ = field.try_extension_type::<BoxType>()?;
        (arr, typ).try_into()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::builder::RectBuilder;
    use crate::eq::rect_eq;
    use geoarrow_schema::Dimension;

    #[test]
    fn rect_array_round_trip() {
        let rect = geo::Rect::new(
            geo::coord! { x: 0.0, y: 5.0 },
            geo::coord! { x: 10.0, y: 15.0 },
        );
        let typ = BoxType::new(Dimension::XY, Default::default());
        let mut builder = RectBuilder::with_capacity(typ, 1);
        builder.push_rect(Some(&rect));
        builder.push_min_max(&rect.min(), &rect.max());
        let rect_arr = builder.finish();

        let arrow_arr = rect_arr.into_array_ref();

        let rect_arr_again = RectArray::try_from((
            arrow_arr.as_ref(),
            BoxType::new(Dimension::XY, Default::default()),
        ))
        .unwrap();
        let rect_again = rect_arr_again.value(0).unwrap();
        assert!(rect_eq(&rect, &rect_again));
    }
}
