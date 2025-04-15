use std::sync::Arc;

use crate::array::{CoordBuffer, WkbArray};
use crate::builder::LineStringBuilder;
use crate::capacity::LineStringCapacity;
use crate::datatypes::GeoArrowType;
use crate::eq::offset_buffer_eq;
use crate::error::{GeoArrowError, Result};
use crate::scalar::LineString;
use crate::trait_::{ArrayAccessor, GeoArrowArray, IntoArrow};
use crate::util::{OffsetBufferUtils, offsets_buffer_i64_to_i32};

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, GenericListArray, OffsetSizeTrait};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field};
use geoarrow_schema::{LineStringType, Metadata};

/// An immutable array of LineString geometries.
///
/// This is semantically equivalent to `Vec<Option<LineString>>` due to the internal validity
/// bitmap.
#[derive(Debug, Clone)]
pub struct LineStringArray {
    pub(crate) data_type: LineStringType,

    pub(crate) coords: CoordBuffer,

    /// Offsets into the coordinate array where each geometry starts
    pub(crate) geom_offsets: OffsetBuffer<i32>,

    /// Validity bitmap
    pub(crate) validity: Option<NullBuffer>,
}

pub(super) fn check(
    coords: &CoordBuffer,
    validity_len: Option<usize>,
    geom_offsets: &OffsetBuffer<i32>,
) -> Result<()> {
    if validity_len.is_some_and(|len| len != geom_offsets.len_proxy()) {
        return Err(GeoArrowError::General(
            "validity mask length must match the number of values".to_string(),
        ));
    }

    if *geom_offsets.last() as usize != coords.len() {
        return Err(GeoArrowError::General(
            "largest geometry offset must match coords length".to_string(),
        ));
    }

    Ok(())
}

impl LineStringArray {
    /// Create a new LineStringArray from parts
    ///
    /// # Implementation
    ///
    /// This function is `O(1)`.
    ///
    /// # Panics
    ///
    /// - if the validity is not `None` and its length is different from the number of geometries
    /// - if the largest geometry offset does not match the number of coordinates
    pub fn new(
        coords: CoordBuffer,
        geom_offsets: OffsetBuffer<i32>,
        validity: Option<NullBuffer>,
        metadata: Arc<Metadata>,
    ) -> Self {
        Self::try_new(coords, geom_offsets, validity, metadata).unwrap()
    }

    /// Create a new LineStringArray from parts
    ///
    /// # Implementation
    ///
    /// This function is `O(1)`.
    ///
    /// # Errors
    ///
    /// - if the validity buffer does not have the same length as the number of geometries
    /// - if the geometry offsets do not match the number of coordinates
    pub fn try_new(
        coords: CoordBuffer,
        geom_offsets: OffsetBuffer<i32>,
        validity: Option<NullBuffer>,
        metadata: Arc<Metadata>,
    ) -> Result<Self> {
        check(&coords, validity.as_ref().map(|v| v.len()), &geom_offsets)?;
        Ok(Self {
            data_type: LineStringType::new(coords.coord_type(), coords.dim(), metadata),
            coords,
            geom_offsets,
            validity,
        })
    }

    /// Access the underlying coordinate buffer
    pub fn coords(&self) -> &CoordBuffer {
        &self.coords
    }

    #[allow(dead_code)]
    pub(crate) fn into_inner(self) -> (CoordBuffer, OffsetBuffer<i32>, Option<NullBuffer>) {
        (self.coords, self.geom_offsets, self.validity)
    }

    /// Access the underlying geometry offsets buffer
    pub fn geom_offsets(&self) -> &OffsetBuffer<i32> {
        &self.geom_offsets
    }

    /// The lengths of each buffer contained in this array.
    pub fn buffer_lengths(&self) -> LineStringCapacity {
        LineStringCapacity::new(*self.geom_offsets.last() as usize, self.len())
    }

    /// The number of bytes occupied by this array.
    pub fn num_bytes(&self) -> usize {
        let validity_len = self.nulls().map(|v| v.buffer().len()).unwrap_or(0);
        validity_len + self.buffer_lengths().num_bytes()
    }

    /// Slices this [`LineStringArray`] in place.
    ///
    /// # Implementation
    /// This operation is `O(1)` as it amounts to increase two ref counts.
    /// # Examples
    /// ```ignore
    /// use arrow::array::PrimitiveArray;
    /// use arrow_array::types::Int32Type;
    ///
    /// let array: PrimitiveArray<Int32Type> = PrimitiveArray::from(vec![1, 2, 3]);
    /// assert_eq!(format!("{:?}", array), "PrimitiveArray<Int32>\n[\n  1,\n  2,\n  3,\n]");
    /// let sliced = array.slice(1, 1);
    /// assert_eq!(format!("{:?}", sliced), "PrimitiveArray<Int32>\n[\n  2,\n]");
    /// // note: `sliced` and `array` share the same memory region.
    /// ```
    /// # Panic
    /// This function panics iff `offset + length > self.len()`.
    #[inline]
    pub fn slice(&self, offset: usize, length: usize) -> Self {
        assert!(
            offset + length <= self.len(),
            "offset + length may not exceed length of array"
        );
        // Note: we **only** slice the geom_offsets and not any actual data. Otherwise the offsets
        // would be in the wrong location.
        Self {
            data_type: self.data_type.clone(),
            coords: self.coords.clone(),
            geom_offsets: self.geom_offsets.slice(offset, length),
            validity: self.validity.as_ref().map(|v| v.slice(offset, length)),
        }
    }
}

impl GeoArrowArray for LineStringArray {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn into_array_ref(self) -> ArrayRef {
        Arc::new(self.into_arrow())
    }

    fn to_array_ref(&self) -> ArrayRef {
        self.clone().into_array_ref()
    }

    #[inline]
    fn len(&self) -> usize {
        self.geom_offsets.len_proxy()
    }

    #[inline]
    fn nulls(&self) -> Option<&NullBuffer> {
        self.validity.as_ref()
    }

    fn data_type(&self) -> GeoArrowType {
        GeoArrowType::LineString(self.data_type.clone())
    }

    fn slice(&self, offset: usize, length: usize) -> Arc<dyn GeoArrowArray> {
        Arc::new(self.slice(offset, length))
    }
}

impl<'a> ArrayAccessor<'a> for LineStringArray {
    type Item = LineString<'a>;

    unsafe fn value_unchecked(&'a self, index: usize) -> Result<Self::Item> {
        Ok(LineString::new(&self.coords, &self.geom_offsets, index))
    }
}

impl IntoArrow for LineStringArray {
    type ArrowArray = GenericListArray<i32>;
    type ExtensionType = LineStringType;

    fn into_arrow(self) -> Self::ArrowArray {
        let vertices_field = match self.data_type.data_type() {
            DataType::List(inner_field) => inner_field,
            _ => unreachable!(),
        };
        let validity = self.validity;
        let coord_array = self.coords.into_array_ref();
        GenericListArray::new(vertices_field, self.geom_offsets, coord_array, validity)
    }

    fn ext_type(&self) -> &Self::ExtensionType {
        &self.data_type
    }
}

impl TryFrom<(&GenericListArray<i32>, LineStringType)> for LineStringArray {
    type Error = GeoArrowError;

    fn try_from((value, typ): (&GenericListArray<i32>, LineStringType)) -> Result<Self> {
        let coords = CoordBuffer::from_arrow(value.values().as_ref(), typ.dimension())?;
        let geom_offsets = value.offsets();
        let validity = value.nulls();

        Ok(Self::new(
            coords,
            geom_offsets.clone(),
            validity.cloned(),
            typ.metadata().clone(),
        ))
    }
}

impl TryFrom<(&GenericListArray<i64>, LineStringType)> for LineStringArray {
    type Error = GeoArrowError;

    fn try_from((value, typ): (&GenericListArray<i64>, LineStringType)) -> Result<Self> {
        let coords = CoordBuffer::from_arrow(value.values().as_ref(), typ.dimension())?;
        let geom_offsets = offsets_buffer_i64_to_i32(value.offsets())?;
        let validity = value.nulls();

        Ok(Self::new(
            coords,
            geom_offsets,
            validity.cloned(),
            typ.metadata().clone(),
        ))
    }
}
impl TryFrom<(&dyn Array, LineStringType)> for LineStringArray {
    type Error = GeoArrowError;

    fn try_from((value, typ): (&dyn Array, LineStringType)) -> Result<Self> {
        match value.data_type() {
            DataType::List(_) => (value.as_list::<i32>(), typ).try_into(),
            DataType::LargeList(_) => (value.as_list::<i64>(), typ).try_into(),
            _ => Err(GeoArrowError::General(format!(
                "Unexpected type: {:?}",
                value.data_type()
            ))),
        }
    }
}

impl TryFrom<(&dyn Array, &Field)> for LineStringArray {
    type Error = GeoArrowError;

    fn try_from((arr, field): (&dyn Array, &Field)) -> Result<Self> {
        let typ = field.try_extension_type::<LineStringType>()?;
        (arr, typ).try_into()
    }
}

impl<O: OffsetSizeTrait> TryFrom<(WkbArray<O>, LineStringType)> for LineStringArray {
    type Error = GeoArrowError;

    fn try_from(value: (WkbArray<O>, LineStringType)) -> Result<Self> {
        let mut_arr: LineStringBuilder = value.try_into()?;
        Ok(mut_arr.finish())
    }
}

impl PartialEq for LineStringArray {
    fn eq(&self, other: &Self) -> bool {
        self.validity == other.validity
            && offset_buffer_eq(&self.geom_offsets, &other.geom_offsets)
            && self.coords == other.coords
    }
}

#[cfg(test)]
mod test {
    use geo_traits::to_geo::ToGeoLineString;
    use geoarrow_schema::{CoordType, Dimension};

    use crate::test::linestring;

    use super::*;

    #[test]
    fn geo_round_trip() {
        for coord_type in [CoordType::Interleaved, CoordType::Separated] {
            let geoms = [Some(linestring::ls0()), None, Some(linestring::ls1()), None];
            let typ = LineStringType::new(coord_type, Dimension::XY, Default::default());
            let geo_arr = LineStringBuilder::from_nullable_line_strings(&geoms, typ).finish();

            for (i, g) in geo_arr.iter().enumerate() {
                assert_eq!(geoms[i], g.transpose().unwrap().map(|g| g.to_line_string()));
            }

            // Test sliced
            for (i, g) in geo_arr.slice(2, 2).iter().enumerate() {
                assert_eq!(
                    geoms[i + 2],
                    g.transpose().unwrap().map(|g| g.to_line_string())
                );
            }
        }
    }

    #[test]
    fn try_from_arrow() {
        for coord_type in [CoordType::Interleaved, CoordType::Separated] {
            for dim in [
                Dimension::XY,
                Dimension::XYZ,
                Dimension::XYM,
                Dimension::XYZM,
            ] {
                let geo_arr = linestring::array(coord_type, dim);

                let ext_type = geo_arr.ext_type().clone();
                let field = ext_type.to_field("geometry", true);

                let arrow_arr = geo_arr.to_array_ref();

                let geo_arr2: LineStringArray = (arrow_arr.as_ref(), ext_type).try_into().unwrap();
                let geo_arr3: LineStringArray = (arrow_arr.as_ref(), &field).try_into().unwrap();

                assert_eq!(geo_arr, geo_arr2);
                assert_eq!(geo_arr, geo_arr3);
            }
        }
    }

    #[test]
    fn partial_eq() {
        let arr1 = linestring::ls_array(CoordType::Interleaved);
        let arr2 = linestring::ls_array(CoordType::Separated);
        assert_eq!(arr1, arr1);
        assert_eq!(arr2, arr2);
        assert_eq!(arr1, arr2);

        assert_ne!(arr1, arr2.slice(0, 2));
    }
}
