use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Array, GenericListArray, OffsetSizeTrait};
use arrow_buffer::bit_iterator::BitIterator;
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field};

use crate::array::zip_validity::ZipValidity;
use crate::array::{CoordBuffer, CoordType, MixedGeometryArray};
use crate::datatypes::GeoDataType;
use crate::scalar::GeometryCollection;
use crate::trait_::{GeometryArrayAccessor, GeometryArraySelfMethods, IntoArrow};
use crate::GeometryArrayTrait;

/// An immutable array of GeometryCollection geometries using GeoArrow's in-memory representation.
///
/// This is semantically equivalent to `Vec<Option<GeometryCollection>>` due to the internal
/// validity bitmap.
#[derive(Debug, Clone)]
pub struct GeometryCollectionArray<O: OffsetSizeTrait> {
    // Always GeoDataType::GeometryCollection or GeoDataType::LargeGeometryCollection
    data_type: GeoDataType,

    pub array: MixedGeometryArray<O>,

    /// Offsets into the mixed geometry array where each geometry starts
    pub geom_offsets: OffsetBuffer<O>,

    /// Validity bitmap
    pub validity: Option<NullBuffer>,
}

impl<O: OffsetSizeTrait> GeometryCollectionArray<O> {
    /// Create a new GeometryCollectionArray from parts
    ///
    /// # Implementation
    ///
    /// This function is `O(1)`.
    pub fn new(
        array: MixedGeometryArray<O>,
        geom_offsets: OffsetBuffer<O>,
        validity: Option<NullBuffer>,
    ) -> Self {
        let coord_type = array.coord_type();
        let data_type = match O::IS_LARGE {
            true => GeoDataType::LargeGeometryCollection(coord_type),
            false => GeoDataType::GeometryCollection(coord_type),
        };

        Self {
            data_type,
            array,
            geom_offsets,
            validity,
        }
    }

    fn mixed_field(&self) -> Arc<Field> {
        self.array.extension_field()
    }

    fn geometries_field(&self) -> Arc<Field> {
        let name = "geometries";
        match O::IS_LARGE {
            true => Field::new_large_list(name, self.mixed_field(), false).into(),
            false => Field::new_list(name, self.mixed_field(), false).into(),
        }
    }
}

impl<O: OffsetSizeTrait> GeometryArrayTrait for GeometryCollectionArray<O> {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn data_type(&self) -> &GeoDataType {
        &self.data_type
    }

    fn storage_type(&self) -> DataType {
        todo!()
    }

    fn extension_field(&self) -> Arc<Field> {
        let mut metadata = HashMap::new();
        metadata.insert(
            "ARROW:extension:name".to_string(),
            self.extension_name().to_string(),
        );
        Arc::new(Field::new("geometry", self.storage_type(), true).with_metadata(metadata))
    }

    fn extension_name(&self) -> &str {
        "geoarrow.geometrycollection"
    }

    fn into_array_ref(self) -> Arc<dyn Array> {
        Arc::new(self.into_arrow())
    }

    fn coord_type(&self) -> CoordType {
        todo!()
    }

    /// Returns the number of geometries in this array
    #[inline]
    fn len(&self) -> usize {
        // TODO: double check/make helper for this
        self.geom_offsets.len() - 1
    }

    /// Returns the optional validity.
    #[inline]
    fn validity(&self) -> Option<&NullBuffer> {
        self.validity.as_ref()
    }
}

impl<O: OffsetSizeTrait> GeometryArraySelfMethods for GeometryCollectionArray<O> {
    fn with_coords(self, _coords: CoordBuffer) -> Self {
        todo!()
    }

    fn into_coord_type(self, _coord_type: CoordType) -> Self {
        todo!()
    }

    /// Slices this [`GeometryCollectionArray`] in place.
    ///
    /// # Implementation
    /// This operation is `O(1)` as it amounts to increase two ref counts.
    /// # Examples
    /// ```
    /// use arrow::array::PrimitiveArray;
    /// use arrow_array::types::Int32Type;
    ///
    /// let array: PrimitiveArray<Int32Type> = PrimitiveArray::from(vec![1, 2, 3]);
    /// assert_eq!(format!("{:?}", array), "PrimitiveArray<Int32>\n[\n  1,\n  2,\n  3,\n]");
    /// let sliced = array.slice(1, 1);
    /// assert_eq!(format!("{:?}", sliced), "PrimitiveArray<Int32>\n[\n  2,\n]");
    ///
    /// // note: `sliced` and `array` share the same memory region.
    /// ```
    /// # Panic
    /// This function panics iff `offset + length > self.len()`.
    #[inline]
    fn slice(&self, offset: usize, length: usize) -> Self {
        assert!(
            offset + length <= self.len(),
            "offset + length may not exceed length of array"
        );
        // Note: we **only** slice the geom_offsets and not any actual data
        Self {
            data_type: self.data_type.clone(),
            array: self.array.clone(),
            geom_offsets: self.geom_offsets.slice(offset, length),
            validity: self.validity.as_ref().map(|v| v.slice(offset, length)),
        }
    }

    fn owned_slice(&self, _offset: usize, _length: usize) -> Self {
        todo!()
    }
}

impl<'a, O: OffsetSizeTrait> GeometryArrayAccessor<'a> for GeometryCollectionArray<O> {
    type Item = GeometryCollection<'a, O>;
    type ItemGeo = geo::GeometryCollection;

    unsafe fn value_unchecked(&'a self, index: usize) -> Self::Item {
        GeometryCollection {
            array: &self.array,
            geom_offsets: &self.geom_offsets,
            geom_index: index,
        }
    }
}

impl<O: OffsetSizeTrait> IntoArrow for GeometryCollectionArray<O> {
    type ArrowArray = GenericListArray<O>;

    fn into_arrow(self) -> Self::ArrowArray {
        let geometries_field = self.geometries_field();
        let validity = self.validity;
        let values = self.array.into_array_ref();
        GenericListArray::new(geometries_field, self.geom_offsets, values, validity)
    }
}

// Implement geometry accessors
impl<O: OffsetSizeTrait> GeometryCollectionArray<O> {
    /// Iterator over geo Geometry objects, not looking at validity
    pub fn iter_geo_values(&self) -> impl Iterator<Item = geo::GeometryCollection> + '_ {
        (0..self.len()).map(|i| self.value_as_geo(i))
    }

    /// Iterator over geo Geometry objects, taking into account validity
    pub fn iter_geo(
        &self,
    ) -> ZipValidity<
        geo::GeometryCollection,
        impl Iterator<Item = geo::GeometryCollection> + '_,
        BitIterator,
    > {
        ZipValidity::new_with_validity(self.iter_geo_values(), self.nulls())
    }

    /// Returns the value at slot `i` as a GEOS geometry.
    #[cfg(feature = "geos")]
    pub fn value_as_geos(&self, i: usize) -> geos::Geometry {
        self.value(i).try_into().unwrap()
    }

    /// Gets the value at slot `i` as a GEOS geometry, additionally checking the validity bitmap
    #[cfg(feature = "geos")]
    pub fn get_as_geos(&self, i: usize) -> Option<geos::Geometry> {
        if self.is_null(i) {
            return None;
        }

        Some(self.value_as_geos(i))
    }

    /// Iterator over GEOS geometry objects
    #[cfg(feature = "geos")]
    pub fn iter_geos_values(&self) -> impl Iterator<Item = geos::Geometry> + '_ {
        (0..self.len()).map(|i| self.value_as_geos(i))
    }

    /// Iterator over GEOS geometry objects, taking validity into account
    #[cfg(feature = "geos")]
    pub fn iter_geos(
        &self,
    ) -> ZipValidity<geos::Geometry, impl Iterator<Item = geos::Geometry> + '_, BitIterator> {
        ZipValidity::new_with_validity(self.iter_geos_values(), self.nulls())
    }
}
