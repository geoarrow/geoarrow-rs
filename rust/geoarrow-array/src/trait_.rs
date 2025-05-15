use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef};
use arrow_buffer::NullBuffer;
use arrow_schema::extension::ExtensionType;
use geo_traits::GeometryTrait;
use geoarrow_schema::Metadata;
use geoarrow_schema::error::GeoArrowResult;

use crate::datatypes::GeoArrowType;

/// Convert GeoArrow arrays into their respective [arrow][arrow_array] arrays.
pub trait IntoArrow {
    /// The type of arrow array that this geoarrow array can be converted into.
    type ArrowArray: Array;

    /// The extension type representing this array. It will always be a type defined by
    /// [geoarrow_schema].
    type ExtensionType: ExtensionType;

    /// Converts this geoarrow array into an arrow array.
    ///
    /// Note that [arrow][arrow_array] arrays do not maintain Arrow extension metadata, so the
    /// result of this method will omit any spatial extension information. Ensure you call
    /// [Self::ext_type] to get extension information that you can add to a
    /// [`Field`][arrow_schema::Field].
    // Return Arc<Self::ArrowArray>? Could that replace `into_array_ref` on the trait?
    fn into_arrow(self) -> Self::ArrowArray;

    /// Return the Arrow extension type representing this array.
    fn ext_type(&self) -> &Self::ExtensionType;
}

/// A base trait for all GeoArrow arrays.
///
/// This is a geospatial corollary to the upstream [`Array`] trait.
pub trait GeoArrowArray: Debug + Send + Sync {
    /// Returns the array as [`Any`] so that it can be downcasted to a specific implementation.
    ///
    /// Prefer using [`AsGeoArrowArray`][crate::cast::AsGeoArrowArray] instead of calling this
    /// method and manually downcasting.
    fn as_any(&self) -> &dyn Any;

    /// Returns the [`GeoArrowType`] of this array.
    ///
    /// # Examples
    ///
    /// ```
    /// # use geoarrow_array::builder::PointBuilder;
    /// # use geoarrow_array::{GeoArrowArray, GeoArrowType};
    /// # use geoarrow_schema::{CoordType, Dimension, PointType};
    /// #
    /// let point = geo_types::point!(x: 1., y: 2.);
    /// let point_type = PointType::new(CoordType::Separated, Dimension::XY, Default::default());
    /// let point_array = PointBuilder::from_points([point].iter(), point_type.clone()).finish();
    /// assert_eq!(point_array.data_type(), GeoArrowType::Point(point_type));
    /// ```
    fn data_type(&self) -> GeoArrowType;

    /// Converts this array into an `Arc`ed [`arrow`][arrow_array] array, consuming the original
    /// array.
    ///
    /// This is `O(1)`.
    ///
    /// Note that **this will omit any spatial extension information**. You must separately store
    /// the spatial information in a [`Field`][arrow_schema::Field] derived from
    /// [`Self::data_type`].
    ///
    /// # Examples
    ///
    /// ```
    /// # use arrow_array::ArrayRef;
    /// # use geoarrow_array::builder::PointBuilder;
    /// # use geoarrow_array::GeoArrowArray;
    /// # use geoarrow_schema::{CoordType, Dimension, PointType};
    /// #
    /// let point = geo_types::point!(x: 1., y: 2.);
    /// let point_type = PointType::new(CoordType::Separated, Dimension::XY, Default::default());
    /// let point_array = PointBuilder::from_points([point].iter(), point_type.clone()).finish();
    /// let array_ref: ArrayRef = point_array.into_array_ref();
    /// ```
    #[must_use]
    fn into_array_ref(self) -> ArrayRef;

    /// Converts this array into an `Arc`ed [`arrow`][arrow_array] array.
    ///
    /// This is `O(1)`.
    ///
    /// Note that **this will omit any spatial extension information**. You must separately store
    /// the spatial information in a [`Field`][arrow_schema::Field] derived from
    /// [`Self::data_type`].
    ///
    /// # Examples
    ///
    /// ```
    /// # use arrow_array::ArrayRef;
    /// # use geoarrow_array::builder::PointBuilder;
    /// # use geoarrow_array::GeoArrowArray;
    /// # use geoarrow_schema::{CoordType, Dimension, PointType};
    /// #
    /// let point = geo_types::point!(x: 1., y: 2.);
    /// let point_type = PointType::new(CoordType::Separated, Dimension::XY, Default::default());
    /// let point_array = PointBuilder::from_points([point].iter(), point_type.clone()).finish();
    /// let array_ref: ArrayRef = point_array.to_array_ref();
    /// ```
    #[must_use]
    fn to_array_ref(&self) -> ArrayRef;

    /// The number of geometries contained in this array.
    ///
    /// # Examples
    ///
    /// ```
    /// # use arrow_array::ArrayRef;
    /// # use geoarrow_array::builder::PointBuilder;
    /// # use geoarrow_array::GeoArrowArray;
    /// # use geoarrow_schema::{CoordType, Dimension, PointType};
    /// #
    /// let point = geo_types::point!(x: 1., y: 2.);
    /// let point_type = PointType::new(CoordType::Separated, Dimension::XY, Default::default());
    /// let point_array = PointBuilder::from_points([point].iter(), point_type.clone()).finish();
    /// assert_eq!(point_array.len(), 1);
    /// ```
    fn len(&self) -> usize;

    /// Returns `true` if the array is empty.
    ///
    /// # Examples
    ///
    /// ```
    /// # use arrow_array::ArrayRef;
    /// # use geoarrow_array::builder::PointBuilder;
    /// # use geoarrow_array::GeoArrowArray;
    /// # use geoarrow_schema::{CoordType, Dimension, PointType};
    /// #
    /// let point = geo_types::point!(x: 1., y: 2.);
    /// let point_type = PointType::new(CoordType::Separated, Dimension::XY, Default::default());
    /// let point_array = PointBuilder::from_points([point].iter(), point_type.clone()).finish();
    /// assert!(!point_array.is_empty());
    /// ```
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns a potentially computed [`NullBuffer``] that represents the logical null values of
    /// this array, if any.
    ///
    /// Logical nulls represent the values that are null in the array, regardless of the underlying
    /// physical arrow representation.
    ///
    /// For most array types, this is equivalent to the "physical" nulls returned by
    /// [`Array::nulls`]. However it is different for union arrays, including our
    /// [`GeometryArray`][crate::array::GeometryArray] and
    /// [`GeometryCollectionArray`][crate::array::GeometryCollectionArray] types, because the
    /// unions aren't encoded in a single null buffer.
    fn logical_nulls(&self) -> Option<NullBuffer>;

    /// Returns the number of null slots in this array.
    ///
    /// This is `O(1)` since the number of null elements is pre-computed.
    ///
    /// # Examples
    ///
    /// ```
    /// # use geoarrow_array::GeoArrowArray;
    /// # use geoarrow_array::builder::PointBuilder;
    /// # use geoarrow_schema::{CoordType, Dimension, PointType};
    /// #
    /// let point = geo_types::point!(x: 1., y: 2.);
    /// let point_type = PointType::new(CoordType::Separated, Dimension::XY, Default::default());
    /// let point_array =
    ///     PointBuilder::from_nullable_points([Some(&point), None].into_iter(), point_type.clone()).finish();
    /// assert_eq!(point_array.logical_null_count(), 1);
    /// ```
    fn logical_null_count(&self) -> usize;

    /// Returns whether slot `i` is null.
    ///
    /// # Examples
    ///
    /// ```
    /// # use geoarrow_array::GeoArrowArray;
    /// # use geoarrow_array::builder::PointBuilder;
    /// # use geoarrow_schema::{CoordType, Dimension, PointType};
    /// #
    /// let point = geo_types::point!(x: 1., y: 2.);
    ///
    /// let point_type = PointType::new(CoordType::Separated, Dimension::XY, Default::default());
    /// let point_array =
    ///     PointBuilder::from_nullable_points([Some(&point), None].into_iter(), point_type.clone()).finish();
    /// assert!(point_array.is_null(1));
    /// ```
    ///
    /// # Panics
    ///
    /// Panics iff `i >= self.len()`.
    fn is_null(&self, i: usize) -> bool;

    /// Returns whether slot `i` is valid.
    ///
    /// # Examples
    ///
    /// ```
    /// # use geoarrow_array::GeoArrowArray;
    /// # use geoarrow_array::builder::PointBuilder;
    /// # use geoarrow_schema::{CoordType, Dimension, PointType};
    /// #
    /// let point = geo_types::point!(x: 1., y: 2.);
    ///
    /// let point_type = PointType::new(CoordType::Separated, Dimension::XY, Default::default());
    /// let point_array =
    ///     PointBuilder::from_nullable_points([Some(&point), None].into_iter(), point_type.clone()).finish();
    /// assert!(point_array.is_valid(0));
    /// ```
    ///
    /// # Panics
    ///
    /// Panics iff `i >= self.len()`.
    #[inline]
    fn is_valid(&self, i: usize) -> bool {
        !self.is_null(i)
    }

    /// Returns a zero-copy slice of this array with the indicated offset and length.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::sync::Arc;
    /// #
    /// # use geoarrow_array::GeoArrowArray;
    /// # use geoarrow_array::builder::PointBuilder;
    /// # use geoarrow_schema::{CoordType, Dimension, PointType};
    /// #
    /// let point1 = geo_types::point!(x: 1., y: 2.);
    /// let point2 = geo_types::point!(x: 3., y: 4.);
    ///
    /// let point_type = PointType::new(CoordType::Separated, Dimension::XY, Default::default());
    /// let point_array =
    ///     Arc::new(PointBuilder::from_points([point1, point2].iter(), point_type.clone()).finish())
    ///         as Arc<dyn GeoArrowArray>;
    /// let sliced_array = point_array.slice(1, 1);
    /// assert_eq!(sliced_array.len(), 1);
    /// ```
    ///
    /// # Panics
    ///
    /// This function panics iff `offset + length > self.len()`.
    #[must_use]
    fn slice(&self, offset: usize, length: usize) -> Arc<dyn GeoArrowArray>;

    /// Change the [`Metadata`] of this array.
    fn with_metadata(self, metadata: Arc<Metadata>) -> Arc<dyn GeoArrowArray>;
}

/// A trait for accessing the values of a [`GeoArrowArray`].
///
/// # Performance
///
/// Accessing a geometry from a "native" array, such as `PointArray`, `MultiPolygonArray` or
/// `GeometryArray` will always be constant-time and zero-copy.
///
/// Accessing a geometry from a "serialized" array such as `GenericWkbArray` or `GenericWktArray`
/// will trigger some amount of parsing. In the case of `GenericWkbArray`, accessing an item will
/// read the WKB header and scan the buffer if needed to find internal geometry offsets, but will
/// not copy any internal coordinates. This allows for later access to be constant-time (though not
/// necessarily zero-copy, since WKB is not byte-aligned). In the case of `GenericWktArray`,
/// accessing a geometry will fully parse the WKT string and copy coordinates to a separate
/// representation. This means that calling `.iter()` on a `GenericWktArray` will transparently
/// fully parse every row.
///
/// # Validity
///
/// A [`GeoArrowArrayAccessor`] must always return a well-defined value for an index that is
/// within the bounds `0..Array::len`, including for null indexes where [`Array::is_null`] is true.
///
/// The value at null indexes is unspecified, and implementations must not rely on a specific
/// value such as [`Default::default`] being returned, however, it must not be undefined.
pub trait GeoArrowArrayAccessor<'a>: GeoArrowArray {
    /// The [geoarrow scalar object][crate::scalar] for this geometry array type.
    type Item: Send + Sync + GeometryTrait<T = f64>;

    /// Returns the element at index `i`, not considering validity.
    ///
    /// # Examples
    ///
    /// ```
    /// use geo_traits::{CoordTrait, PointTrait};
    /// # use geoarrow_array::GeoArrowArrayAccessor;
    /// # use geoarrow_array::builder::PointBuilder;
    /// # use geoarrow_schema::{CoordType, Dimension, PointType};
    ///
    /// let point1 = geo_types::point!(x: 1., y: 2.);
    ///
    /// let point_type = PointType::new(CoordType::Separated, Dimension::XY, Default::default());
    /// let point_array =
    ///     PointBuilder::from_nullable_points([Some(&point1), None].into_iter(), point_type.clone())
    ///         .finish();
    ///
    /// let coord = point_array.value(0).unwrap().coord().unwrap();
    /// assert_eq!(coord.x(), 1.);
    /// assert_eq!(coord.y(), 2.);
    /// ```
    ///
    /// # Errors
    ///
    /// Errors for invalid WKT and WKB geometries. Will never error for native arrays.
    ///
    /// # Panics
    ///
    /// Panics if the value is outside the bounds of the array.
    fn value(&'a self, index: usize) -> GeoArrowResult<Self::Item> {
        assert!(index <= self.len());
        unsafe { self.value_unchecked(index) }
    }

    /// Returns the element at index `i`, not considering validity.
    ///
    /// # Examples
    ///
    /// ```
    /// use geo_traits::{CoordTrait, PointTrait};
    /// # use geoarrow_array::GeoArrowArrayAccessor;
    /// # use geoarrow_array::builder::PointBuilder;
    /// # use geoarrow_schema::{CoordType, Dimension, PointType};
    ///
    /// let point1 = geo_types::point!(x: 1., y: 2.);
    ///
    /// let point_type = PointType::new(CoordType::Separated, Dimension::XY, Default::default());
    /// let point_array =
    ///     PointBuilder::from_nullable_points([Some(&point1), None].into_iter(), point_type.clone())
    ///         .finish();
    ///
    /// let coord = unsafe { point_array.value_unchecked(0) }
    ///     .unwrap()
    ///     .coord()
    ///     .unwrap();
    /// assert_eq!(coord.x(), 1.);
    /// assert_eq!(coord.y(), 2.);
    /// ```
    ///
    /// # Errors
    ///
    /// Errors for invalid WKT and WKB geometries. Will never error for native arrays.
    ///
    /// # Safety
    ///
    /// Caller is responsible for ensuring that the index is within the bounds of the array
    unsafe fn value_unchecked(&'a self, index: usize) -> GeoArrowResult<Self::Item>;

    /// Returns the value at slot `i` as an Arrow scalar, considering validity.
    ///
    /// # Examples
    ///
    /// ```
    /// # use geoarrow_array::GeoArrowArrayAccessor;
    /// # use geoarrow_array::builder::PointBuilder;
    /// # use geoarrow_schema::{CoordType, Dimension, PointType};
    /// #
    /// let point1 = geo_types::point!(x: 1., y: 2.);
    ///
    /// let point_type = PointType::new(CoordType::Separated, Dimension::XY, Default::default());
    /// let point_array =
    ///     PointBuilder::from_nullable_points([Some(&point1), None].into_iter(), point_type.clone())
    ///         .finish();
    ///
    /// assert!(point_array.get(0).unwrap().is_some());
    /// assert!(point_array.get(1).unwrap().is_none());
    /// ```
    ///
    /// # Errors
    ///
    /// Errors for invalid WKT and WKB geometries. Will never error for native arrays.
    fn get(&'a self, index: usize) -> GeoArrowResult<Option<Self::Item>> {
        if self.is_null(index) {
            return Ok(None);
        }

        Ok(Some(self.value(index)?))
    }

    /// Returns the value at slot `i` as an Arrow scalar, considering validity.
    ///
    /// # Errors
    ///
    /// Errors for invalid WKT and WKB geometries. Will never error for native arrays.
    ///
    /// # Safety
    ///
    /// Caller is responsible for ensuring that the index is within the bounds of the array
    unsafe fn get_unchecked(&'a self, index: usize) -> Option<GeoArrowResult<Self::Item>> {
        if self.is_null(index) {
            return None;
        }

        Some(unsafe { self.value_unchecked(index) })
    }

    /// Iterates over this array's geoarrow scalar values, considering validity.
    ///
    /// # Errors
    ///
    /// Errors for invalid WKT and WKB geometries. Will never error for native arrays.
    fn iter(&'a self) -> impl ExactSizeIterator<Item = Option<GeoArrowResult<Self::Item>>> + 'a {
        (0..self.len()).map(|i| unsafe { self.get_unchecked(i) })
    }

    /// Iterator over geoarrow scalar values, not considering validity.
    ///
    /// # Errors
    ///
    /// Errors for invalid WKT and WKB geometries. Will never error for native arrays.
    fn iter_values(&'a self) -> impl ExactSizeIterator<Item = GeoArrowResult<Self::Item>> + 'a {
        (0..self.len()).map(|i| unsafe { self.value_unchecked(i) })
    }
}

/// A trait describing a mutable geometry array; i.e. an array whose values can be changed.
///
// Note: This trait is not yet publicly exported from this crate, as we're not sure how the API
// should be, and in particular whether we need this trait to be dyn-compatible or not.
pub(crate) trait GeoArrowArrayBuilder: Debug + Send + Sync {
    /// Returns the length of the array.
    fn len(&self) -> usize;

    /// Returns whether the array is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Push a null value to this builder.
    fn push_null(&mut self);

    /// Push a geometry to this builder.
    #[allow(dead_code)]
    fn push_geometry(
        &mut self,
        geometry: Option<&impl GeometryTrait<T = f64>>,
    ) -> GeoArrowResult<()>;

    /// Finish the builder and return an [`Arc`] to the resulting array.
    #[allow(dead_code)]
    fn finish(self) -> Arc<dyn GeoArrowArray>;
}
