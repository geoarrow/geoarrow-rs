//! Defines [`GeometryArrayTrait`], which all geometry arrays implement.

use crate::array::{CoordBuffer, CoordType};
use arrow_array::{Array, ArrayRef};
use arrow_buffer::{NullBuffer, NullBufferBuilder};
use arrow_schema::{DataType, Field};
use std::any::Any;
use std::sync::Arc;

/// A trait of common methods that all geometry arrays in this crate implement.
pub trait GeometryArrayTrait<'a> {
    /// The [geoarrow scalar object][crate::scalar] for this geometry array type.
    type Scalar: GeometryScalarTrait<'a>;

    /// The [`geo`] scalar object for this geometry array type.
    type ScalarGeo: From<Self::Scalar>;

    /// The [`arrow2` array][arrow2::array] that corresponds to this geometry array.
    type ArrowArray;

    /// Access the value at slot `i` as an Arrow scalar, not considering validity.
    fn value_unchecked(&'a self, i: usize) -> Self::Scalar {
        self.value(i)
    }

    /// Access the value at slot `i` as an Arrow scalar, not considering validity.
    fn value(&'a self, i: usize) -> Self::Scalar;

    /// Access the value at slot `i` as an Arrow scalar, considering validity.
    fn get(&'a self, i: usize) -> Option<Self::Scalar> {
        if self.is_null(i) {
            return None;
        }

        Some(self.value(i))
    }

    /// Access the value at slot `i` as a [`geo`] scalar, not considering validity.
    fn value_as_geo(&'a self, i: usize) -> Self::ScalarGeo {
        self.value(i).into()
    }

    /// Access the value at slot `i` as a [`geo`] scalar, considering validity.
    fn get_as_geo(&'a self, i: usize) -> Option<Self::ScalarGeo> {
        if self.is_null(i) {
            return None;
        }

        Some(self.value_as_geo(i))
    }

    /// Get the logical DataType of this array.
    fn storage_type(&self) -> DataType;

    /// Get the extension type of this array, as [defined by the GeoArrow
    /// specification](https://github.com/geoarrow/geoarrow/blob/main/extension-types.md).
    ///
    /// Always returns `DataType::Extension`.
    fn extension_field(&self) -> Arc<Field>;

    /// Convert this array into an [`arrow2`] array.
    /// # Implementation
    /// This is `O(1)`.
    fn into_arrow(self) -> Self::ArrowArray;

    /// Convert this array into a boxed [`arrow2`] array.
    /// # Implementation
    /// This is `O(1)`.
    fn into_array_ref(self) -> ArrayRef;

    /// Create a new array with replaced coordinates
    ///
    /// This is useful if you want to apply an operation to _every_ coordinate in unison, such as a
    /// reprojection or a scaling operation, with no regards to each individual geometry
    fn with_coords(self, coords: CoordBuffer) -> Self;

    /// Get the coordinate type of this geometry array, either interleaved or separated.
    fn coord_type(&self) -> CoordType;

    /// Cast the coordinate buffer of this geometry array to the given coordinate type.
    fn into_coord_type(self, coord_type: CoordType) -> Self;

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
        self.nulls()
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

    /// Returns a zero-copy slice of this array with the indicated offset and length.
    ///
    /// # Panic
    /// This function panics iff `offset + length > self.len()`.
    fn slice(&self, offset: usize, length: usize) -> Self;

    /// A slice that fully copies the contents of the underlying buffer
    fn owned_slice(&self, offset: usize, length: usize) -> Self;

    // /// Clones this [`GeometryArray`] with a new new assigned bitmap.
    // /// # Panic
    // /// This function panics iff `validity.len() != self.len()`.
    // fn with_validity(&self, validity: Option<NullBuffer>) -> Box<dyn GeometryArray>;

    /// Clones this array to an owned, boxed geometry array.
    fn to_boxed(&self) -> Box<Self>;
}

pub trait GeometryScalarTrait<'a> {
    /// The [`geo`] scalar object for this geometry array type.
    type ScalarGeo;

    fn to_geo(&self) -> Self::ScalarGeo;
}

/// A trait describing a mutable geometry array; i.e. an array whose values can be changed.
/// Mutable arrays cannot be cloned but can be mutated in place,
/// thereby making them useful to perform numeric operations without allocations.
/// As in [`GeometryArrayTrait`], concrete arrays (such as
/// [`MutablePointArray`][crate::array::MutablePointArray]) implement how they are mutated.
pub trait MutableGeometryArray: std::fmt::Debug + Send + Sync {
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

    /// Convert to `Any`, to enable dynamic casting.
    fn as_any(&self) -> &dyn Any;

    /// Convert to mutable `Any`, to enable dynamic casting.
    fn as_mut_any(&mut self) -> &mut dyn Any;

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
