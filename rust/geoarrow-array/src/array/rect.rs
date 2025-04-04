use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Float64Type;
use arrow_array::{Array, ArrayRef, StructArray};
use arrow_buffer::{NullBuffer, ScalarBuffer};
use arrow_schema::extension::ExtensionType;
use arrow_schema::{DataType, Field};
use geoarrow_schema::{BoxType, CoordType, Dimension, Metadata};

use crate::datatypes::NativeType;
use crate::error::GeoArrowError;
use crate::rect::RectBuilder;
use crate::scalar::Rect;
use crate::trait_::{ArrayAccessor, GeometryArraySelfMethods, IntoArrow};
use crate::{ArrayBase, NativeArray};
use crate::{CoordBuffer, SeparatedCoordBuffer};
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
    data_type: BoxType,

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

impl ArrayBase for RectArray {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn storage_type(&self) -> DataType {
        self.data_type.data_type()
    }

    fn extension_field(&self) -> Arc<Field> {
        self.data_type.to_field("geometry", true).into()
    }

    fn extension_name(&self) -> &str {
        BoxType::NAME
    }

    fn into_array_ref(self) -> ArrayRef {
        Arc::new(self.into_arrow())
    }

    fn to_array_ref(&self) -> ArrayRef {
        self.clone().into_array_ref()
    }

    fn metadata(&self) -> Arc<Metadata> {
        self.data_type.metadata().clone()
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
        NativeType::Rect(self.data_type.clone())
    }

    fn coord_type(&self) -> CoordType {
        CoordType::Separated
    }

    fn to_coord_type(&self, _coord_type: CoordType) -> Arc<dyn NativeArray> {
        Arc::new(self.clone())
    }

    fn with_metadata(&self, metadata: Arc<Metadata>) -> crate::trait_::NativeArrayRef {
        let mut arr = self.clone();
        arr.data_type = self.data_type.clone().with_metadata(metadata);
        Arc::new(arr)
    }

    fn as_ref(&self) -> &dyn NativeArray {
        self
    }

    fn slice(&self, offset: usize, length: usize) -> Arc<dyn NativeArray> {
        Arc::new(self.slice(offset, length))
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

    unsafe fn value_unchecked(&'a self, index: usize) -> Self::Item {
        Rect::new(&self.lower, &self.upper, index)
    }
}

impl IntoArrow for RectArray {
    type ArrowArray = StructArray;

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
}

impl TryFrom<(&StructArray, Dimension)> for RectArray {
    type Error = GeoArrowError;

    fn try_from((value, dim): (&StructArray, Dimension)) -> Result<Self, Self::Error> {
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
        let metadata = Arc::new(Metadata::try_from(field)?);
        arr.data_type = arr.data_type.clone().with_metadata(metadata);
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

#[cfg(test)]
mod test {
    use super::*;
    use crate::eq::rect_eq;
    use crate::RectBuilder;
    use geoarrow_schema::Dimension;

    #[test]
    fn rect_array_round_trip() {
        let rect = geo::Rect::new(
            geo::coord! { x: 0.0, y: 5.0 },
            geo::coord! { x: 10.0, y: 15.0 },
        );
        let mut builder =
            RectBuilder::with_capacity_and_options(Dimension::XY, 1, Default::default());
        builder.push_rect(Some(&rect));
        builder.push_min_max(&rect.min(), &rect.max());
        let rect_arr = builder.finish();

        let arrow_arr = rect_arr.into_array_ref();
        let rect_arr_again = RectArray::try_from((arrow_arr.as_ref(), Dimension::XY)).unwrap();
        let rect_again = rect_arr_again.value(0);
        assert!(rect_eq(&rect, &rect_again));
    }
}
