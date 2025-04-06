use std::any::Any;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef};
use arrow_buffer::NullBuffer;
use arrow_schema::extension::ExtensionType;

use crate::datatypes::{NativeType, SerializedType};

/// Convert GeoArrow arrays into their respective arrow arrays.
pub trait IntoArrow {
    /// The type of arrow array that this geoarrow array can be converted into.
    type ArrowArray: Array;
    type ExtensionType: ExtensionType;

    /// Converts this geoarrow array into an arrow array.
    fn into_arrow(self) -> Self::ArrowArray;

    fn ext_type(&self) -> &Self::ExtensionType;
}

/// A base trait that both [NativeArray] and [SerializedArray] implement
pub trait ArrayBase: std::fmt::Debug + Send + Sync {
    /// Returns the array as [`Any`] so that it can be
    /// downcasted to a specific implementation.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use std::sync::Arc;
    /// use arrow_array::{Int32Array, RecordBatch};
    /// use arrow_schema::{Schema, Field, DataType, ArrowError};
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

    /// Converts this array into an arced [`arrow`] array, consuming the original array.
    ///
    /// This is `O(1)`.
    ///
    /// Note that **this will omit any spatial extension information**.
    ///
    /// # Examples
    ///
    /// ```ignore
    ///
    /// use geoarrow::{array::PointArray, ArrayBase};
    /// use geoarrow_schema::Dimension;
    ///
    /// let point = geo::point!(x: 1., y: 2.);
    /// let point_array: PointArray = (vec![point].as_slice(), Dimension::XY).into();
    /// let array_ref = point_array.into_array_ref();
    /// ```
    #[must_use]
    fn into_array_ref(self) -> ArrayRef;

    /// Converts this array into an arced [`arrow`] array.
    ///
    /// This is `O(1)`.
    ///
    /// Note that **this will omit any spatial extension information**.
    ///
    /// # Examples
    ///
    /// ```ignore
    ///
    /// use geoarrow::{array::PointArray, ArrayBase};
    /// use geoarrow_schema::Dimension;
    ///
    /// let point = geo::point!(x: 1., y: 2.);
    /// let point_array: PointArray = (vec![point].as_slice(), Dimension::XY).into();
    /// let array_ref = point_array.to_array_ref();
    /// ```
    #[must_use]
    fn to_array_ref(&self) -> ArrayRef;

    /// The number of geometries contained in this array.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use geoarrow::{array::PointArray, ArrayBase};
    /// use geoarrow_schema::Dimension;
    ///
    /// let point = geo::point!(x: 1., y: 2.);
    /// let point_array: PointArray = (vec![point].as_slice(), Dimension::XY).into();
    /// assert_eq!(point_array.len(), 1);
    /// ```
    fn len(&self) -> usize;

    /// Returns `true` if the array is empty.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use geoarrow::{array::PointArray, ArrayBase};
    /// use geoarrow_schema::Dimension;
    ///
    /// let point = geo::point!(x: 1., y: 2.);
    /// let point_array: PointArray = (vec![point].as_slice(), Dimension::XY).into();
    /// assert!(!point_array.is_empty());
    /// ```
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns an optional reference to the array's nulls buffer.
    ///
    /// Every array has an optional [`NullBuffer`] that, when available
    /// specifies whether the array slot is valid or not (null). When the
    /// validity is [`None`], all slots are valid.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use geoarrow::{ArrayBase, array::PointArray};
    /// use geoarrow_schema::Dimension;
    ///
    /// let point = geo::point!(x: 1., y: 2.);
    /// let array: PointArray = (vec![point].as_slice(), Dimension::XY).into();
    /// assert!(array.nulls().is_none());
    /// ```
    fn nulls(&self) -> Option<&NullBuffer>;

    /// Returns the number of null slots in this array.
    ///
    /// This is `O(1)` since the number of null elements is pre-computed.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use geoarrow::{ArrayBase, array::PointArray};
    /// use geoarrow_schema::Dimension;
    ///
    /// let point = geo::point!(x: 1., y: 2.);
    /// let array: PointArray = (vec![point].as_slice(), Dimension::XY).into();
    /// assert_eq!(array.null_count(), 0);
    /// ```
    #[inline]
    fn null_count(&self) -> usize {
        self.nulls().map(|x| x.null_count()).unwrap_or(0)
    }

    /// Returns whether slot `i` is null.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use geoarrow::{ArrayBase, array::PointArray};
    /// use geoarrow_schema::Dimension;
    ///
    /// let point = geo::point!(x: 1., y: 2.);
    /// let array: PointArray = (vec![point].as_slice(), Dimension::XY).into();
    /// assert!(!array.is_null(0));
    /// ```
    ///
    /// # Panics
    ///
    /// Panics iff `i >= self.len()`.
    #[inline]
    fn is_null(&self, i: usize) -> bool {
        self.nulls().map(|x| x.is_null(i)).unwrap_or(false)
    }

    /// Returns whether slot `i` is valid.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use geoarrow::{ArrayBase, array::PointArray};
    /// use geoarrow_schema::Dimension;
    ///
    /// let point = geo::point!(x: 1., y: 2.);
    /// let array: PointArray = (vec![point].as_slice(), Dimension::XY).into();
    /// assert!(array.is_valid(0));
    /// ```
    ///
    /// # Panics
    ///
    /// Panics iff `i >= self.len()`.
    #[inline]
    fn is_valid(&self, i: usize) -> bool {
        !self.is_null(i)
    }

    // /// Returns the element at index `i`, not considering validity.
    // ///
    // /// # Examples
    // ///
    // /// ```ignore
    // /// use geoarrow::{trait_::ArrayAccessor, array::PointArray};
    // /// use geoarrow_schema::Dimension;
    // ///
    // /// let point = geo::point!(x: 1., y: 2.);
    // /// let array: PointArray = (vec![point].as_slice(), Dimension::XY).into();
    // /// unsafe {
    // ///     let value = array.value_unchecked(0); // geoarrow::scalar::Point
    // /// }
    // /// ```
    // ///
    // /// # Safety
    // ///
    // /// Caller is responsible for ensuring that the index is within the bounds of the array
    // unsafe fn value_unchecked_as_geometry(&self, index: usize) -> impl GeometryTrait<T = f64>;
}

/// A trait for accessing the values of a [`NativeArray`].
///
/// # Validity
///
/// An [`ArrayAccessor`] must always return a well-defined value for an index that is
/// within the bounds `0..Array::len`, including for null indexes where [`Array::is_null`] is true.
///
/// The value at null indexes is unspecified, and implementations must not rely on a specific
/// value such as [`Default::default`] being returned, however, it must not be undefined.
pub trait ArrayAccessor<'a>: ArrayBase {
    /// The [geoarrow scalar object][crate::scalar] for this geometry array type.
    type Item: Send + Sync;

    /// Returns the element at index `i`, not considering validity.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use geoarrow::{trait_::ArrayAccessor, array::PointArray};
    /// use geo_traits::{PointTrait, CoordTrait};
    /// use geoarrow_schema::Dimension;
    ///
    /// let point = geo::point!(x: 1., y: 2.);
    /// let array: PointArray = (vec![point].as_slice(), Dimension::XY).into();
    /// let value = array.value(0); // geoarrow::scalar::Point
    /// assert_eq!(value.coord().unwrap().x(), 1.);
    /// assert_eq!(value.coord().unwrap().y(), 2.);
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if the value is outside the bounds of the array.
    fn value(&'a self, index: usize) -> Self::Item {
        assert!(index <= self.len());
        unsafe { self.value_unchecked(index) }
    }

    /// Returns the element at index `i`, not considering validity.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use geoarrow::{trait_::ArrayAccessor, array::PointArray};
    /// use geoarrow_schema::Dimension;
    ///
    /// let point = geo::point!(x: 1., y: 2.);
    /// let array: PointArray = (vec![point].as_slice(), Dimension::XY).into();
    /// unsafe {
    ///     let value = array.value_unchecked(0); // geoarrow::scalar::Point
    /// }
    /// ```
    ///
    /// # Safety
    ///
    /// Caller is responsible for ensuring that the index is within the bounds of the array
    unsafe fn value_unchecked(&'a self, index: usize) -> Self::Item;

    /// Returns the value at slot `i` as an Arrow scalar, considering validity.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use geoarrow::{trait_::ArrayAccessor, array::PointArray};
    /// use geoarrow_schema::Dimension;
    ///
    /// let point = geo::point!(x: 1., y: 2.);
    /// let array: PointArray = (vec![point].as_slice(), Dimension::XY).into();
    /// assert!(array.get(0).is_some());
    /// ```
    fn get(&'a self, index: usize) -> Option<Self::Item> {
        if self.is_null(index) {
            return None;
        }

        Some(self.value(index))
    }

    /// Returns the value at slot `i` as an Arrow scalar, considering validity.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use geoarrow::{trait_::ArrayAccessor, array::PointArray};
    /// use geoarrow_schema::Dimension;
    ///
    /// let point = geo::point!(x: 1., y: 2.);
    /// let array: PointArray = (vec![point].as_slice(), Dimension::XY).into();
    /// unsafe {
    ///     assert!(array.get_unchecked(0).is_some());
    /// }
    /// ```
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

    /// Iterates over this array's geoarrow scalar values, considering validity.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use geoarrow::{trait_::ArrayAccessor, array::PointArray};
    /// use geoarrow_schema::Dimension;
    ///
    /// let point = geo::point!(x: 1., y: 2.);
    /// let array: PointArray = (vec![point].as_slice(), Dimension::XY).into();
    /// let maybe_points: Vec<Option<_>> = array.iter().collect();
    /// ```
    fn iter(&'a self) -> impl ExactSizeIterator<Item = Option<Self::Item>> + 'a {
        (0..self.len()).map(|i| unsafe { self.get_unchecked(i) })
    }

    /// Iterator over geoarrow scalar values, not considering validity.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use geoarrow::{trait_::ArrayAccessor, array::PointArray};
    /// use geoarrow_schema::Dimension;
    ///
    /// let point = geo::point!(x: 1., y: 2.);
    /// let array: PointArray = (vec![point].as_slice(), Dimension::XY).into();
    /// let points: Vec<_> = array.iter_values().collect();
    /// ```
    fn iter_values(&'a self) -> impl ExactSizeIterator<Item = Self::Item> + 'a {
        (0..self.len()).map(|i| unsafe { self.value_unchecked(i) })
    }
}

/// A trait to represent native-encoded GeoArrow arrays
///
/// This encompasses the core GeoArrow [native encoding](https://github.com/geoarrow/geoarrow/blob/main/format.md#native-encoding) types.
///
/// This trait is often used for downcasting. If you have a dynamically-typed `Arc<dyn
/// NativeArray>`, to downcast into a strongly-typed chunked array use `as_any` with the
/// `data_type` method to discern which chunked array type to pass to `downcast_ref`.
pub trait NativeArray: ArrayBase {
    /// Returns the [`NativeType`] of this array.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use geoarrow::{array::PointArray, datatypes::NativeType, NativeArray};
    /// use geoarrow_schema::Dimension;
    ///
    /// let point = geo::point!(x: 1., y: 2.);
    /// let point_array: PointArray = (vec![point].as_slice(), Dimension::XY).into();
    /// assert!(matches!(point_array.data_type(), NativeType::Point(_, _)));
    /// ```
    fn data_type(&self) -> NativeType;

    /// Returns a zero-copy slice of this array with the indicated offset and length.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use geoarrow::{
    ///     array::PointArray,
    ///     trait_::{GeometryArraySelfMethods, ArrayAccessor, NativeArray, ArrayBase}
    /// };
    /// use geoarrow_schema::Dimension;
    ///
    /// let point_0 = geo::point!(x: 1., y: 2.);
    /// let point_1 = geo::point!(x: 3., y: 4.);
    /// let array: PointArray = (vec![point_0, point_1].as_slice(), Dimension::XY).into();
    /// let smaller_array = array.slice(1, 1);
    /// assert_eq!(smaller_array.len(), 1);
    /// let value = smaller_array.value_as_geo(0);
    /// assert_eq!(value.x(), 3.);
    /// assert_eq!(value.y(), 4.);
    /// ```
    ///
    /// # Panics
    ///
    /// This function panics iff `offset + length > self.len()`.
    #[must_use]
    fn slice(&self, offset: usize, length: usize) -> Arc<dyn NativeArray>;
}

/// A trait to represent serialized GeoArrow arrays
///
/// This encompasses WKB and WKT GeoArrow types.
pub trait SerializedArray: ArrayBase {
    /// Returns a the [`SerializedType`] of this array.
    fn data_type(&self) -> SerializedType;
}

/// A trait describing a mutable geometry array; i.e. an array whose values can be changed.
///
/// Mutable arrays cannot be cloned but can be mutated in place,
/// thereby making them useful to perform numeric operations without allocations.
/// As in [`NativeArray`], concrete arrays (such as
/// [`PointBuilder`][crate::array::PointBuilder]) implement how they are mutated.
pub trait GeometryArrayBuilder: std::fmt::Debug + Send + Sync + Sized {
    /// Returns the length of the array.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use geoarrow::{array::PointBuilder, trait_::GeometryArrayBuilder};
    /// use geoarrow_schema::Dimension;
    ///
    /// let mut builder = PointBuilder::new(Dimension::XY);
    /// assert_eq!(builder.len(), 0);
    /// builder.push_point(Some(&geo::point!(x: 1., y: 2.)));
    /// assert_eq!(builder.len(), 1);
    /// ```
    fn len(&self) -> usize;

    /// Returns whether the array is empty.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use geoarrow::{array::PointBuilder, trait_::GeometryArrayBuilder};
    /// use geoarrow_schema::Dimension;
    ///
    /// let mut builder = PointBuilder::new(Dimension::XY);
    /// assert!(builder.is_empty());
    /// builder.push_point(Some(&geo::point!(x: 1., y: 2.)));
    /// assert!(!builder.is_empty());
    /// ```
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
