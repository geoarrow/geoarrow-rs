//! Defines [`GeometryArrayTrait`], which all geometry arrays implement.

use crate::array::metadata::ArrayMetadata;
use crate::array::{CoordBuffer, CoordType};
use crate::datatypes::GeoDataType;
use arrow_array::{Array, ArrayRef};
use arrow_buffer::{NullBuffer, NullBufferBuilder};
use arrow_schema::{DataType, Field};
use std::any::Any;
use std::sync::Arc;

/// A trait of common methods that all geometry arrays in this crate implement.
///
/// This trait is often used for downcasting. If you have a dynamically-typed `Arc<dyn
/// GeometryArrayTrait>`, to downcast into a strongly-typed chunked array use `as_any` with the
/// `data_type` method to discern which chunked array type to pass to `downcast_ref`.
pub trait GeometryArrayTrait: std::fmt::Debug + Send + Sync {
    /// Returns the array as [`Any`] so that it can be
    /// downcasted to a specific implementation.
    ///
    /// # Example:
    ///
    /// ```
    /// //use geoarrow::datatypes::GeoDataType;
    /// //use geoarrow::array::PointArray;
    /// //use geoarrow::GeometryArrayTrait;
    /// //use geo::point;
    ///
    /// //let point = point!(x: 1., y: 2.);
    /// //let point_array: PointArray = vec![point].into();
    ///
    /// //let geometry_array = Arc::new(point_array) as Arc<dyn GeometryArrayTrait>;
    ///
    /// # use std::sync::Arc;
    /// # use arrow_array::{Int32Array, RecordBatch};
    /// # use arrow_schema::{Schema, Field, DataType, ArrowError};
    ///
    /// let id = Int32Array::from(vec![1, 2, 3, 4, 5]);
    /// let batch = RecordBatch::try_new(
    ///     Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
    ///     vec![Arc::new(id)]
    /// ).unwrap();
    ///
    /// let int32array = batch
    ///     .column(0)
    ///     .as_any()
    ///     .downcast_ref::<Int32Array>()
    ///     .expect("Failed to downcast");
    /// ```
    fn as_any(&self) -> &dyn Any;

    /// Returns a reference to the [`GeoDataType`] of this array.
    ///
    /// # Example:
    ///
    /// ```
    /// use geoarrow::datatypes::GeoDataType;
    /// use geoarrow::array::PointArray;
    /// use geoarrow::GeometryArrayTrait;
    /// use geo::point;
    ///
    /// let point = point!(x: 1., y: 2.);
    /// let point_array: PointArray = vec![point].as_slice().into();
    ///
    /// assert!(matches!(point_array.data_type(), GeoDataType::Point(_)));
    /// ```
    fn data_type(&self) -> &GeoDataType;

    /// Get the logical DataType of this array.
    fn storage_type(&self) -> DataType;

    /// Get the extension type of this array, as [defined by the GeoArrow
    /// specification](https://github.com/geoarrow/geoarrow/blob/main/extension-types.md).
    ///
    /// Always returns `DataType::Extension`.
    fn extension_field(&self) -> Arc<Field>;

    /// Get the extension name of this array.
    fn extension_name(&self) -> &str;

    /// Convert this array into an arced [`arrow`] array.
    /// # Implementation
    /// This is `O(1)`.
    fn into_array_ref(self) -> ArrayRef;

    fn to_array_ref(&self) -> ArrayRef;

    /// Get the coordinate type of this geometry array, either interleaved or separated.
    fn coord_type(&self) -> CoordType;

    /// The number of geometries contained in this array.
    fn len(&self) -> usize;

    /// Returns `true` if the array is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Access the array's validity. Every array has an optional [`NullBuffer`] that, when available
    /// specifies whether the array slot is valid or not (null). When the validity is [`None`], all
    /// slots are valid.
    fn validity(&self) -> Option<&NullBuffer>;

    fn nulls(&self) -> Option<&NullBuffer> {
        self.validity()
    }

    fn logical_nulls(&self) -> Option<NullBuffer> {
        self.nulls().cloned()
    }

    fn metadata(&self) -> Arc<ArrayMetadata>;

    /// The number of null slots in this array.
    /// # Implementation
    /// This is `O(1)` since the number of null elements is pre-computed.
    #[inline]
    fn null_count(&self) -> usize {
        self.nulls().map(|x| x.null_count()).unwrap_or(0)
    }

    /// Returns whether slot `i` is null.
    /// # Panic
    /// Panics iff `i >= self.len()`.
    #[inline]
    fn is_null(&self, i: usize) -> bool {
        self.nulls().map(|x| x.is_null(i)).unwrap_or(false)
    }

    /// Returns whether slot `i` is valid.
    /// # Panic
    /// Panics iff `i >= self.len()`.
    #[inline]
    fn is_valid(&self, i: usize) -> bool {
        !self.is_null(i)
    }

    fn as_ref(&self) -> &dyn GeometryArrayTrait;

    // /// Clones this [`GeometryArray`] with a new new assigned bitmap.
    // /// # Panic
    // /// This function panics iff `validity.len() != self.len()`.
    // fn with_validity(&self, validity: Option<NullBuffer>) -> Box<dyn GeometryArray>;
}

/// A generic trait for accessing the values of an [`Array`]
///
/// # Validity
///
/// An [`ArrayAccessor`] must always return a well-defined value for an index that is
/// within the bounds `0..Array::len`, including for null indexes where [`Array::is_null`] is true.
///
/// The value at null indexes is unspecified, and implementations must not rely on a specific
/// value such as [`Default::default`] being returned, however, it must not be undefined
pub trait GeometryArrayAccessor<'a>: GeometryArrayTrait {
    /// The [geoarrow scalar object][crate::scalar] for this geometry array type.
    type Item: Send + Sync + GeometryScalarTrait;

    /// The [`geo`] scalar object for this geometry array type.
    type ItemGeo: From<Self::Item>;

    /// Returns the element at index `i`
    /// # Panics
    /// Panics if the value is outside the bounds of the array
    fn value(&'a self, index: usize) -> Self::Item {
        assert!(index <= self.len());
        unsafe { self.value_unchecked(index) }
    }

    /// Returns the element at index `i`
    /// # Safety
    /// Caller is responsible for ensuring that the index is within the bounds of the array
    unsafe fn value_unchecked(&'a self, index: usize) -> Self::Item;

    /// Access the value at slot `i` as an Arrow scalar, considering validity.
    fn get(&'a self, index: usize) -> Option<Self::Item> {
        if self.is_null(index) {
            return None;
        }

        Some(self.value(index))
    }

    /// Access the value at slot `i` as an Arrow scalar, considering validity.
    ///
    /// # Safety
    ///
    /// Caller is responsible for ensuring that the index is within the bounds of the array
    unsafe fn get_unchecked(&'a self, index: usize) -> Option<Self::Item> {
        if self.is_null(index) {
            return None;
        }

        Some(unsafe { self.value_unchecked(index) })
    }

    /// Access the value at slot `i` as a [`geo`] scalar, not considering validity.
    fn value_as_geo(&'a self, i: usize) -> Self::ItemGeo {
        self.value(i).into()
    }

    /// Access the value at slot `i` as a [`geo`] scalar, considering validity.
    fn get_as_geo(&'a self, i: usize) -> Option<Self::ItemGeo> {
        if self.is_null(i) {
            return None;
        }

        Some(self.value_as_geo(i))
    }

    fn iter(&'a self) -> impl Iterator<Item = Option<Self::Item>> + 'a {
        (0..self.len()).map(|i| unsafe { self.get_unchecked(i) })
    }

    /// Iterator over geoarrow scalar values, not looking at validity
    fn iter_values(&'a self) -> impl Iterator<Item = Self::Item> + ExactSizeIterator + 'a {
        (0..self.len()).map(|i| unsafe { self.value_unchecked(i) })
    }

    /// Iterator over geo scalar values, taking into account validity
    fn iter_geo(&'a self) -> impl Iterator<Item = Option<Self::ItemGeo>> + 'a {
        (0..self.len()).map(|i| unsafe { self.get_unchecked(i) }.map(|x| x.into()))
    }

    /// Iterator over geo scalar values, not looking at validity
    fn iter_geo_values(&'a self) -> impl Iterator<Item = Self::ItemGeo> + 'a {
        (0..self.len()).map(|i| unsafe { self.value_unchecked(i) }.into())
    }
}

/// Horrible name, to be changed to a better name in the future!!
pub trait GeometryArraySelfMethods {
    /// Create a new array with replaced coordinates
    ///
    /// This is useful if you want to apply an operation to _every_ coordinate in unison, such as a
    /// reprojection or a scaling operation, with no regards to each individual geometry
    fn with_coords(self, coords: CoordBuffer) -> Self;

    /// Cast the coordinate buffer of this geometry array to the given coordinate type.
    fn into_coord_type(self, coord_type: CoordType) -> Self;

    /// Returns a zero-copy slice of this array with the indicated offset and length.
    ///
    /// # Panic
    /// This function panics iff `offset + length > self.len()`.
    #[must_use]
    fn slice(&self, offset: usize, length: usize) -> Self;

    /// A slice that fully copies the contents of the underlying buffer
    #[must_use]
    fn owned_slice(&self, offset: usize, length: usize) -> Self;
}

pub trait IntoArrow {
    type ArrowArray;

    fn into_arrow(self) -> Self::ArrowArray;
}

pub trait GeometryScalarTrait {
    /// The [`geo`] scalar object for this geometry array type.
    type ScalarGeo;

    fn to_geo(&self) -> Self::ScalarGeo;

    #[cfg(feature = "geos")]
    fn to_geos(&self) -> std::result::Result<geos::Geometry, geos::Error>;
}

/// A trait describing a mutable geometry array; i.e. an array whose values can be changed.
/// Mutable arrays cannot be cloned but can be mutated in place,
/// thereby making them useful to perform numeric operations without allocations.
/// As in [`GeometryArrayTrait`], concrete arrays (such as
/// [`PointBuilder`][crate::array::PointBuilder]) implement how they are mutated.
pub trait GeometryArrayBuilder: std::fmt::Debug + Send + Sync + Sized {
    /// The length of the array.
    fn len(&self) -> usize;

    /// Whether the array is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The optional validity of the array.
    fn validity(&self) -> &NullBufferBuilder;

    fn new() -> Self;

    fn with_geom_capacity_and_options(
        geom_capacity: usize,
        coord_type: CoordType,
        metadata: Arc<ArrayMetadata>,
    ) -> Self;

    fn with_geom_capacity(geom_capacity: usize) -> Self {
        GeometryArrayBuilder::with_geom_capacity_and_options(
            geom_capacity,
            Default::default(),
            Default::default(),
        )
    }

    fn set_metadata(&mut self, metadata: Arc<ArrayMetadata>);

    fn finish(self) -> Arc<dyn GeometryArrayTrait>;

    /// Get the coordinate type of this geometry array, either interleaved or separated.
    fn coord_type(&self) -> CoordType;

    fn metadata(&self) -> Arc<ArrayMetadata>;

    // /// Convert itself to an (immutable) [`GeometryArray`].
    // fn as_box(&mut self) -> Box<GeometryArrayTrait>;

    // /// Convert itself to an (immutable) atomically reference counted [`GeometryArray`].
    // // This provided implementation has an extra allocation as it first
    // // boxes `self`, then converts the box into an `Arc`. Implementors may wish
    // // to avoid an allocation by skipping the box completely.
    // fn as_arc(&mut self) -> std::sync::Arc<GeometryArrayTrait> {
    //     self.as_box().into()
    // }

    // /// Adds a new null element to the array.
    // fn push_null(&mut self);

    // /// Whether `index` is valid / set.
    // /// # Panic
    // /// Panics if `index >= self.len()`.
    // #[inline]
    // fn is_valid(&self, index: usize) -> bool {
    //     self.validity()
    //         .as_ref()
    //         .map(|x| x.get(index))
    //         .unwrap_or(true)
    // }

    // /// Reserves additional slots to its capacity.
    // fn reserve(&mut self, additional: usize);

    // /// Shrink the array to fit its length.
    // fn shrink_to_fit(&mut self);

    fn into_array_ref(self) -> Arc<dyn Array>;
}
