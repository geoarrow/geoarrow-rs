use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, GenericListArray, OffsetSizeTrait};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field};
use geoarrow_schema::{CoordType, Metadata, MultiPointType};

use crate::array::{CoordBuffer, PointArray, GenericWkbArray};
use crate::builder::MultiPointBuilder;
use crate::capacity::MultiPointCapacity;
use crate::datatypes::GeoArrowType;
use crate::eq::offset_buffer_eq;
use crate::error::{GeoArrowError, Result};
use crate::scalar::MultiPoint;
use crate::trait_::{GeoArrowArray, GeoArrowArrayAccessor, IntoArrow};
use crate::util::{OffsetBufferUtils, offsets_buffer_i64_to_i32};

/// An immutable array of MultiPoint geometries.
///
/// This is semantically equivalent to `Vec<Option<MultiPoint>>` due to the internal validity
/// bitmap.
#[derive(Debug, Clone)]
pub struct MultiPointArray {
    pub(crate) data_type: MultiPointType,

    pub(crate) coords: CoordBuffer,

    /// Offsets into the coordinate array where each geometry starts
    pub(crate) geom_offsets: OffsetBuffer<i32>,

    /// Validity bitmap
    pub(crate) nulls: Option<NullBuffer>,
}

pub(super) fn check(
    coords: &CoordBuffer,
    validity_len: Option<usize>,
    geom_offsets: &OffsetBuffer<i32>,
) -> Result<()> {
    if validity_len.is_some_and(|len| len != geom_offsets.len_proxy()) {
        return Err(GeoArrowError::General(
            "nulls mask length must match the number of values".to_string(),
        ));
    }

    if *geom_offsets.last() as usize != coords.len() {
        return Err(GeoArrowError::General(
            "largest geometry offset must match coords length".to_string(),
        ));
    }

    Ok(())
}

impl MultiPointArray {
    /// Create a new MultiPointArray from parts
    ///
    /// # Implementation
    ///
    /// This function is `O(1)`.
    ///
    /// # Panics
    ///
    /// - if the nulls is not `None` and its length is different from the number of geometries
    /// - if the largest geometry offset does not match the number of coordinates
    pub fn new(
        coords: CoordBuffer,
        geom_offsets: OffsetBuffer<i32>,
        nulls: Option<NullBuffer>,
        metadata: Arc<Metadata>,
    ) -> Self {
        Self::try_new(coords, geom_offsets, nulls, metadata).unwrap()
    }

    /// Create a new MultiPointArray from parts
    ///
    /// # Implementation
    ///
    /// This function is `O(1)`.
    ///
    /// # Errors
    ///
    /// - if the nulls is not `None` and its length is different from the number of geometries
    /// - if the geometry offsets do not match the number of coordinates
    pub fn try_new(
        coords: CoordBuffer,
        geom_offsets: OffsetBuffer<i32>,
        nulls: Option<NullBuffer>,
        metadata: Arc<Metadata>,
    ) -> Result<Self> {
        check(&coords, nulls.as_ref().map(|v| v.len()), &geom_offsets)?;
        Ok(Self {
            data_type: MultiPointType::new(coords.coord_type(), coords.dim(), metadata),
            coords,
            geom_offsets,
            nulls,
        })
    }

    fn vertices_field(&self) -> Arc<Field> {
        Field::new("points", self.coords.storage_type(), false).into()
    }

    /// Access the underlying coord buffer
    pub fn coords(&self) -> &CoordBuffer {
        &self.coords
    }

    #[allow(dead_code)]
    pub(crate) fn into_inner(self) -> (CoordBuffer, OffsetBuffer<i32>, Option<NullBuffer>) {
        (self.coords, self.geom_offsets, self.nulls)
    }

    /// Access the underlying geometry offsets buffer
    pub fn geom_offsets(&self) -> &OffsetBuffer<i32> {
        &self.geom_offsets
    }

    /// The lengths of each buffer contained in this array.
    pub fn buffer_lengths(&self) -> MultiPointCapacity {
        MultiPointCapacity::new(*self.geom_offsets.last() as usize, self.len())
    }

    /// The number of bytes occupied by this array.
    pub fn num_bytes(&self) -> usize {
        let validity_len = self.nulls.as_ref().map(|v| v.buffer().len()).unwrap_or(0);
        validity_len + self.buffer_lengths().num_bytes()
    }

    /// Slices this [`MultiPointArray`] in place.
    ///
    /// # Implementation
    ///
    /// This operation is `O(1)` as it amounts to increasing a few ref counts.
    ///
    /// # Panic
    ///
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
            nulls: self.nulls.as_ref().map(|v| v.slice(offset, length)),
        }
    }

    /// Change the [`CoordType`] of this array.
    pub fn into_coord_type(self, coord_type: CoordType) -> Self {
        Self {
            data_type: self.data_type.with_coord_type(coord_type),
            coords: self.coords.into_coord_type(coord_type),
            ..self
        }
    }

    /// Change the [`Metadata`] of this array.
    pub fn with_metadata(self, metadata: Arc<Metadata>) -> Self {
        Self {
            data_type: self.data_type.with_metadata(metadata),
            ..self
        }
    }
}

impl GeoArrowArray for MultiPointArray {
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
    fn logical_nulls(&self) -> Option<NullBuffer> {
        self.nulls.clone()
    }

    #[inline]
    fn logical_null_count(&self) -> usize {
        self.nulls.as_ref().map(|v| v.null_count()).unwrap_or(0)
    }

    #[inline]
    fn is_null(&self, i: usize) -> bool {
        self.nulls
            .as_ref()
            .map(|n| n.is_null(i))
            .unwrap_or_default()
    }

    fn data_type(&self) -> GeoArrowType {
        GeoArrowType::MultiPoint(self.data_type.clone())
    }

    fn slice(&self, offset: usize, length: usize) -> Arc<dyn GeoArrowArray> {
        Arc::new(self.slice(offset, length))
    }

    fn with_metadata(self, metadata: Arc<Metadata>) -> Arc<dyn GeoArrowArray> {
        Arc::new(self.with_metadata(metadata))
    }
}

impl<'a> GeoArrowArrayAccessor<'a> for MultiPointArray {
    type Item = MultiPoint<'a>;

    unsafe fn value_unchecked(&'a self, index: usize) -> Result<Self::Item> {
        Ok(MultiPoint::new(&self.coords, &self.geom_offsets, index))
    }
}

impl IntoArrow for MultiPointArray {
    type ArrowArray = GenericListArray<i32>;
    type ExtensionType = MultiPointType;

    fn into_arrow(self) -> Self::ArrowArray {
        let vertices_field = self.vertices_field();
        let nulls = self.nulls;
        let coord_array = self.coords.into();
        GenericListArray::new(vertices_field, self.geom_offsets, coord_array, nulls)
    }

    fn ext_type(&self) -> &Self::ExtensionType {
        &self.data_type
    }
}

impl TryFrom<(&GenericListArray<i32>, MultiPointType)> for MultiPointArray {
    type Error = GeoArrowError;

    fn try_from((value, typ): (&GenericListArray<i32>, MultiPointType)) -> Result<Self> {
        let coords = CoordBuffer::from_arrow(value.values().as_ref(), typ.dimension())?;
        let geom_offsets = value.offsets();
        let nulls = value.nulls();

        Ok(Self::new(
            coords,
            geom_offsets.clone(),
            nulls.cloned(),
            typ.metadata().clone(),
        ))
    }
}

impl TryFrom<(&GenericListArray<i64>, MultiPointType)> for MultiPointArray {
    type Error = GeoArrowError;

    fn try_from((value, typ): (&GenericListArray<i64>, MultiPointType)) -> Result<Self> {
        let coords = CoordBuffer::from_arrow(value.values().as_ref(), typ.dimension())?;
        let geom_offsets = offsets_buffer_i64_to_i32(value.offsets())?;
        let nulls = value.nulls();

        Ok(Self::new(
            coords,
            geom_offsets,
            nulls.cloned(),
            typ.metadata().clone(),
        ))
    }
}

impl TryFrom<(&dyn Array, MultiPointType)> for MultiPointArray {
    type Error = GeoArrowError;

    fn try_from((value, typ): (&dyn Array, MultiPointType)) -> Result<Self> {
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

impl TryFrom<(&dyn Array, &Field)> for MultiPointArray {
    type Error = GeoArrowError;

    fn try_from((arr, field): (&dyn Array, &Field)) -> Result<Self> {
        let typ = field.try_extension_type::<MultiPointType>()?;
        (arr, typ).try_into()
    }
}

impl<O: OffsetSizeTrait> TryFrom<(GenericWkbArray<O>, MultiPointType)> for MultiPointArray {
    type Error = GeoArrowError;

    fn try_from(value: (GenericWkbArray<O>, MultiPointType)) -> Result<Self> {
        let mut_arr: MultiPointBuilder = value.try_into()?;
        Ok(mut_arr.finish())
    }
}

impl From<PointArray> for MultiPointArray {
    fn from(value: PointArray) -> Self {
        let (coord_type, dimension, metadata) = value.data_type.into_inner();
        let new_type = MultiPointType::new(coord_type, dimension, metadata);

        let coords = value.coords;
        let geom_offsets = OffsetBuffer::from_lengths(vec![1; coords.len()]);
        let nulls = value.nulls;
        Self {
            data_type: new_type,
            coords,
            geom_offsets,
            nulls,
        }
    }
}

impl PartialEq for MultiPointArray {
    fn eq(&self, other: &Self) -> bool {
        self.nulls == other.nulls
            && offset_buffer_eq(&self.geom_offsets, &other.geom_offsets)
            && self.coords == other.coords
    }
}

#[cfg(test)]
mod test {
    use geo_traits::to_geo::ToGeoMultiPoint;
    use geoarrow_schema::{CoordType, Dimension};

    use crate::test::multipoint;

    use super::*;

    #[test]
    fn geo_round_trip() {
        for coord_type in [CoordType::Interleaved, CoordType::Separated] {
            let geoms = [Some(multipoint::mp0()), None, Some(multipoint::mp1()), None];
            let typ = MultiPointType::new(coord_type, Dimension::XY, Default::default());
            let geo_arr = MultiPointBuilder::from_nullable_multi_points(&geoms, typ).finish();

            for (i, g) in geo_arr.iter().enumerate() {
                assert_eq!(geoms[i], g.transpose().unwrap().map(|g| g.to_multi_point()));
            }

            // Test sliced
            for (i, g) in geo_arr.slice(2, 2).iter().enumerate() {
                assert_eq!(
                    geoms[i + 2],
                    g.transpose().unwrap().map(|g| g.to_multi_point())
                );
            }
        }
    }

    #[test]
    fn geo_round_trip2() {
        for coord_type in [CoordType::Interleaved, CoordType::Separated] {
            let geo_arr = multipoint::array(coord_type, Dimension::XY);
            let geo_geoms = geo_arr
                .iter()
                .map(|x| x.transpose().unwrap().map(|g| g.to_multi_point()))
                .collect::<Vec<_>>();

            let typ = MultiPointType::new(coord_type, Dimension::XY, Default::default());
            let geo_arr2 = MultiPointBuilder::from_nullable_multi_points(&geo_geoms, typ).finish();
            assert_eq!(geo_arr, geo_arr2);
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
                let geo_arr = multipoint::array(coord_type, dim);

                let ext_type = geo_arr.ext_type().clone();
                let field = ext_type.to_field("geometry", true);

                let arrow_arr = geo_arr.to_array_ref();

                let geo_arr2: MultiPointArray = (arrow_arr.as_ref(), ext_type).try_into().unwrap();
                let geo_arr3: MultiPointArray = (arrow_arr.as_ref(), &field).try_into().unwrap();

                assert_eq!(geo_arr, geo_arr2);
                assert_eq!(geo_arr, geo_arr3);
            }
        }
    }

    #[test]
    fn partial_eq() {
        for dim in [
            Dimension::XY,
            Dimension::XYZ,
            Dimension::XYM,
            Dimension::XYZM,
        ] {
            let arr1 = multipoint::array(CoordType::Interleaved, dim);
            let arr2 = multipoint::array(CoordType::Separated, dim);
            assert_eq!(arr1, arr1);
            assert_eq!(arr2, arr2);
            assert_eq!(arr1, arr2);

            assert_ne!(arr1, arr2.slice(0, 2));
        }
    }
}
