//! Defines [`GeometryArrayTrait`], which all geometry arrays implement.

use crate::array::{CoordBuffer, CoordType};
use crate::datatypes::GeoDataType;
use arrow_array::{Array, ArrayRef};
use arrow_buffer::{NullBuffer, NullBufferBuilder};
use arrow_schema::{DataType, Field};
use std::any::Any;
use std::sync::Arc;

/// A trait of common methods that all geometry arrays in this crate implement.
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

    /// Returns a reference to the [`DataType`] of this array.
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
}

/// A trait describing a mutable geometry array; i.e. an array whose values can be changed.
/// Mutable arrays cannot be cloned but can be mutated in place,
/// thereby making them useful to perform numeric operations without allocations.
/// As in [`GeometryArrayTrait`], concrete arrays (such as
/// [`PointBuilder`][crate::array::PointBuilder]) implement how they are mutated.
pub trait GeometryArrayBuilder: std::fmt::Debug + Send + Sync {
    /// The length of the array.
    fn len(&self) -> usize;

    /// Whether the array is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The optional validity of the array.
    fn validity(&self) -> &NullBufferBuilder;

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
