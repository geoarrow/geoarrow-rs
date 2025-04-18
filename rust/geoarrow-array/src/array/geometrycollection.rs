use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, GenericListArray, OffsetSizeTrait};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field};
use geoarrow_schema::{CoordType, GeometryCollectionType, Metadata};

use crate::array::{MixedGeometryArray, WkbArray};
use crate::builder::GeometryCollectionBuilder;
use crate::capacity::GeometryCollectionCapacity;
use crate::datatypes::GeoArrowType;
use crate::eq::offset_buffer_eq;
use crate::error::{GeoArrowError, Result};
use crate::scalar::GeometryCollection;
use crate::trait_::{ArrayAccessor, GeoArrowArray, IntoArrow};
use crate::util::{OffsetBufferUtils, offsets_buffer_i64_to_i32};

/// An immutable array of GeometryCollection geometries.
///
/// This is semantically equivalent to `Vec<Option<GeometryCollection>>` due to the internal
/// validity bitmap.
#[derive(Debug, Clone)]
pub struct GeometryCollectionArray {
    pub(crate) data_type: GeometryCollectionType,

    pub(crate) array: MixedGeometryArray,

    /// Offsets into the mixed geometry array where each geometry starts
    pub(crate) geom_offsets: OffsetBuffer<i32>,

    /// Validity bitmap
    pub(crate) nulls: Option<NullBuffer>,
}

impl GeometryCollectionArray {
    /// Create a new GeometryCollectionArray from parts
    ///
    /// # Implementation
    ///
    /// This function is `O(1)`.
    pub fn new(
        array: MixedGeometryArray,
        geom_offsets: OffsetBuffer<i32>,
        nulls: Option<NullBuffer>,
        metadata: Arc<Metadata>,
    ) -> Self {
        let coord_type = array.coord_type;
        Self {
            data_type: GeometryCollectionType::new(coord_type, array.dim, metadata),
            array,
            geom_offsets,
            nulls,
        }
    }

    fn geometries_field(&self) -> Arc<Field> {
        Field::new("geometries", self.array.storage_type(), false).into()
    }

    /// The lengths of each buffer contained in this array.
    pub fn buffer_lengths(&self) -> GeometryCollectionCapacity {
        GeometryCollectionCapacity::new(
            self.array.buffer_lengths(),
            *self.geom_offsets.last() as usize,
        )
    }

    /// The number of bytes occupied by this array.
    pub fn num_bytes(&self) -> usize {
        let validity_len = self.nulls.as_ref().map(|v| v.buffer().len()).unwrap_or(0);
        validity_len + self.buffer_lengths().num_bytes()
    }

    /// Slices this [`GeometryCollectionArray`] in place.
    ///
    /// # Implementation
    ///
    /// This operation is `O(1)` as it amounts to increasing a few ref counts.
    ///
    /// # Panic
    /// This function panics iff `offset + length > self.len()`.
    #[inline]
    pub fn slice(&self, offset: usize, length: usize) -> Self {
        assert!(
            offset + length <= self.len(),
            "offset + length may not exceed length of array"
        );
        // Note: we **only** slice the geom_offsets and not any actual data
        Self {
            data_type: self.data_type.clone(),
            array: self.array.clone(),
            geom_offsets: self.geom_offsets.slice(offset, length),
            nulls: self.nulls.as_ref().map(|v| v.slice(offset, length)),
        }
    }

    /// Change the [`CoordType`] of this array.
    pub fn into_coord_type(self, coord_type: CoordType) -> Self {
        let metadata = self.data_type.metadata().clone();
        Self::new(
            self.array.into_coord_type(coord_type),
            self.geom_offsets,
            self.nulls,
            metadata,
        )
    }
}

impl GeoArrowArray for GeometryCollectionArray {
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
        GeoArrowType::GeometryCollection(self.data_type.clone())
    }

    fn slice(&self, offset: usize, length: usize) -> Arc<dyn GeoArrowArray> {
        Arc::new(self.slice(offset, length))
    }
}

impl<'a> ArrayAccessor<'a> for GeometryCollectionArray {
    type Item = GeometryCollection<'a>;

    unsafe fn value_unchecked(&'a self, index: usize) -> Result<Self::Item> {
        Ok(GeometryCollection::new(
            &self.array,
            &self.geom_offsets,
            index,
        ))
    }
}

impl IntoArrow for GeometryCollectionArray {
    type ArrowArray = GenericListArray<i32>;
    type ExtensionType = GeometryCollectionType;

    fn into_arrow(self) -> Self::ArrowArray {
        let geometries_field = self.geometries_field();
        let nulls = self.nulls;
        let values = self.array.into_array_ref();
        GenericListArray::new(geometries_field, self.geom_offsets, values, nulls)
    }

    fn ext_type(&self) -> &Self::ExtensionType {
        &self.data_type
    }
}

impl TryFrom<(&GenericListArray<i32>, GeometryCollectionType)> for GeometryCollectionArray {
    type Error = GeoArrowError;

    fn try_from((value, typ): (&GenericListArray<i32>, GeometryCollectionType)) -> Result<Self> {
        let geoms: MixedGeometryArray =
            (value.values().as_ref(), typ.dimension(), typ.coord_type()).try_into()?;
        let geom_offsets = value.offsets();
        let nulls = value.nulls();

        Ok(Self::new(
            geoms,
            geom_offsets.clone(),
            nulls.cloned(),
            typ.metadata().clone(),
        ))
    }
}

impl TryFrom<(&GenericListArray<i64>, GeometryCollectionType)> for GeometryCollectionArray {
    type Error = GeoArrowError;

    fn try_from((value, typ): (&GenericListArray<i64>, GeometryCollectionType)) -> Result<Self> {
        let geoms: MixedGeometryArray =
            (value.values().as_ref(), typ.dimension(), typ.coord_type()).try_into()?;
        let geom_offsets = offsets_buffer_i64_to_i32(value.offsets())?;
        let nulls = value.nulls();

        Ok(Self::new(
            geoms,
            geom_offsets,
            nulls.cloned(),
            typ.metadata().clone(),
        ))
    }
}

impl TryFrom<(&dyn Array, GeometryCollectionType)> for GeometryCollectionArray {
    type Error = GeoArrowError;

    fn try_from((value, typ): (&dyn Array, GeometryCollectionType)) -> Result<Self> {
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

impl TryFrom<(&dyn Array, &Field)> for GeometryCollectionArray {
    type Error = GeoArrowError;

    fn try_from((arr, field): (&dyn Array, &Field)) -> Result<Self> {
        let typ = field.try_extension_type::<GeometryCollectionType>()?;
        (arr, typ).try_into()
    }
}

impl<O: OffsetSizeTrait> TryFrom<(WkbArray<O>, GeometryCollectionType)>
    for GeometryCollectionArray
{
    type Error = GeoArrowError;

    fn try_from(value: (WkbArray<O>, GeometryCollectionType)) -> Result<Self> {
        let mut_arr: GeometryCollectionBuilder = value.try_into()?;
        Ok(mut_arr.finish())
    }
}

impl PartialEq for GeometryCollectionArray {
    fn eq(&self, other: &Self) -> bool {
        self.nulls == other.nulls
            && offset_buffer_eq(&self.geom_offsets, &other.geom_offsets)
            && self.array == other.array
    }
}

#[cfg(test)]
mod test {
    use geoarrow_schema::{CoordType, Dimension};
    use geoarrow_test::raw;

    use crate::test::geometrycollection;

    use super::*;

    #[test]
    fn try_from_arrow() {
        for coord_type in [CoordType::Interleaved, CoordType::Separated] {
            for dim in [
                Dimension::XY,
                Dimension::XYZ,
                Dimension::XYM,
                Dimension::XYZM,
            ] {
                for prefer_multi in [true, false] {
                    let geo_arr = geometrycollection::array(coord_type, dim, prefer_multi);

                    let point_type = geo_arr.ext_type().clone();
                    let field = point_type.to_field("geometry", true);

                    let arrow_arr = geo_arr.to_array_ref();

                    let geo_arr2: GeometryCollectionArray =
                        (arrow_arr.as_ref(), point_type).try_into().unwrap();
                    let geo_arr3: GeometryCollectionArray =
                        (arrow_arr.as_ref(), &field).try_into().unwrap();

                    assert_eq!(geo_arr, geo_arr2);
                    assert_eq!(geo_arr, geo_arr3);
                }
            }
        }
    }

    #[test]
    fn test_nullability() {
        let geoms = raw::geometrycollection::xy::geoms();
        let null_idxs = geoms
            .iter()
            .enumerate()
            .filter_map(|(i, geom)| if geom.is_none() { Some(i) } else { None })
            .collect::<Vec<_>>();

        let typ =
            GeometryCollectionType::new(CoordType::Interleaved, Dimension::XY, Default::default());
        let geo_arr =
            GeometryCollectionBuilder::from_nullable_geometry_collections(&geoms, typ, false)
                .unwrap()
                .finish();

        for null_idx in &null_idxs {
            assert!(geo_arr.is_null(*null_idx));
        }
    }

    #[test]
    fn test_logical_nulls() {
        let geoms = raw::geometrycollection::xy::geoms();
        let expected_nulls = NullBuffer::from_iter(geoms.iter().map(|g| g.is_some()));

        let typ =
            GeometryCollectionType::new(CoordType::Interleaved, Dimension::XY, Default::default());
        let geo_arr =
            GeometryCollectionBuilder::from_nullable_geometry_collections(&geoms, typ, false)
                .unwrap()
                .finish();

        assert_eq!(geo_arr.logical_nulls().unwrap(), expected_nulls);
    }
}
