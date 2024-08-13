//! Defines [`GeometryArrayTrait`], which all geometry arrays implement, and other traits.

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
    /// # Examples
    ///
    /// ```
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

    /// Returns a the [`GeoDataType`] of this array.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::{array::PointArray, datatypes::GeoDataType, GeometryArrayTrait};
    ///
    /// let point = geo::point!(x: 1., y: 2.);
    /// let point_array: PointArray<2> = vec![point].as_slice().into();
    /// assert!(matches!(point_array.data_type(), GeoDataType::Point(_, _)));
    /// ```
    fn data_type(&self) -> GeoDataType;

    /// Returns the physical [DataType] of this array.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::{array::PointArray, datatypes::GeoDataType, GeometryArrayTrait};
    /// use arrow_schema::DataType;
    ///
    /// let point = geo::point!(x: 1., y: 2.);
    /// let point_array: PointArray<2> = vec![point].as_slice().into();
    /// assert!(matches!(point_array.storage_type(), DataType::FixedSizeList(_, _)));
    /// ```
    fn storage_type(&self) -> DataType;

    /// Returns the extension type of this array, as [defined by the GeoArrow
    /// specification](https://github.com/geoarrow/geoarrow/blob/main/extension-types.md).
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::{array::PointArray, GeometryArrayTrait};
    ///
    /// let point = geo::point!(x: 1., y: 2.);
    /// let point_array: PointArray<2> = vec![point].as_slice().into();
    /// let field = point_array.extension_field();
    /// assert_eq!(field.name(), "geometry");
    /// ```
    fn extension_field(&self) -> Arc<Field>;

    /// Returns the extension name of this array.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::{array::PointArray, GeometryArrayTrait};
    ///
    /// let point = geo::point!(x: 1., y: 2.);
    /// let point_array: PointArray<2> = vec![point].as_slice().into();
    /// assert_eq!(point_array.extension_name(), "geoarrow.point");
    /// ```
    fn extension_name(&self) -> &str;

    /// Converts this array into an arced [`arrow`] array, consuming the original array.
    ///
    /// This is `O(1)`.
    ///
    /// # Examples
    ///
    /// ```
    ///
    /// use geoarrow::{array::PointArray, GeometryArrayTrait};
    ///
    /// let point = geo::point!(x: 1., y: 2.);
    /// let point_array: PointArray<2> = vec![point].as_slice().into();
    /// let array_ref = point_array.into_array_ref();
    /// ```
    #[must_use]
    fn into_array_ref(self) -> ArrayRef;

    /// Converts this array into an arced [`arrow`] array.
    ///
    /// This is `O(1)`.
    ///
    /// # Examples
    ///
    /// ```
    ///
    /// use geoarrow::{array::PointArray, GeometryArrayTrait};
    ///
    /// let point = geo::point!(x: 1., y: 2.);
    /// let point_array: PointArray<2> = vec![point].as_slice().into();
    /// let array_ref = point_array.to_array_ref();
    /// ```
    #[must_use]
    fn to_array_ref(&self) -> ArrayRef;

    /// Returns the [CoordType] of this geometry array.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::{array::{PointArray, CoordType}, GeometryArrayTrait};
    ///
    /// let point = geo::point!(x: 1., y: 2.);
    /// let point_array: PointArray<2> = vec![point].as_slice().into();
    /// assert_eq!(point_array.coord_type(), CoordType::Interleaved);
    /// ```
    fn coord_type(&self) -> CoordType;

    /// Converts this array to the same type of array but with the provided [CoordType].
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::{array::{PointArray, CoordType}, GeometryArrayTrait};
    ///
    /// let point = geo::point!(x: 1., y: 2.);
    /// let point_array: PointArray<2> = vec![point].as_slice().into();
    /// let point_array = point_array.to_coord_type(CoordType::Separated);
    /// ```
    #[must_use]
    fn to_coord_type(&self, coord_type: CoordType) -> Arc<dyn GeometryArrayTrait>;

    /// The number of geometries contained in this array.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::{array::PointArray, GeometryArrayTrait};
    ///
    /// let point = geo::point!(x: 1., y: 2.);
    /// let point_array: PointArray<2> = vec![point].as_slice().into();
    /// assert_eq!(point_array.len(), 1);
    /// ```
    fn len(&self) -> usize;

    /// Returns `true` if the array is empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::{array::PointArray, GeometryArrayTrait};
    ///
    /// let point = geo::point!(x: 1., y: 2.);
    /// let point_array: PointArray<2> = vec![point].as_slice().into();
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
    /// ```
    /// use geoarrow::{GeometryArrayTrait, array::PointArray};
    ///
    /// let point = geo::point!(x: 1., y: 2.);
    /// let array: PointArray<2> = vec![point].as_slice().into();
    /// assert!(array.nulls().is_none());
    /// ```
    fn nulls(&self) -> Option<&NullBuffer>;

    /// Returns this array's metadata.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::{GeometryArrayTrait, array::PointArray};
    ///
    /// let point = geo::point!(x: 1., y: 2.);
    /// let array: PointArray<2> = vec![point].as_slice().into();
    /// let metadata = array.metadata();
    /// ```
    fn metadata(&self) -> Arc<ArrayMetadata>;

    /// Returns a geometry array reference that includes the provided metadata.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::{GeometryArrayTrait, array::{PointArray, metadata::{ArrayMetadata, Edges}}};
    ///
    /// let point = geo::point!(x: 1., y: 2.);
    /// let array: PointArray<2> = vec![point].as_slice().into();
    /// let metadata = ArrayMetadata {
    ///     crs: None,
    ///     edges: Some(Edges::Spherical),
    /// };
    /// let metadata = array.with_metadata(metadata.into());
    /// ```
    #[must_use]
    fn with_metadata(&self, metadata: Arc<ArrayMetadata>) -> GeometryArrayRef;

    /// Returns the number of null slots in this array.
    ///
    /// This is `O(1)` since the number of null elements is pre-computed.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::{GeometryArrayTrait, array::PointArray};
    ///
    /// let point = geo::point!(x: 1., y: 2.);
    /// let array: PointArray<2> = vec![point].as_slice().into();
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
    /// ```
    /// use geoarrow::{GeometryArrayTrait, array::PointArray};
    ///
    /// let point = geo::point!(x: 1., y: 2.);
    /// let array: PointArray<2> = vec![point].as_slice().into();
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
    /// ```
    /// use geoarrow::{GeometryArrayTrait, array::PointArray};
    ///
    /// let point = geo::point!(x: 1., y: 2.);
    /// let array: PointArray<2> = vec![point].as_slice().into();
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

    /// Returns a reference to this array.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::{GeometryArrayTrait, array::PointArray};
    ///
    /// let point = geo::point!(x: 1., y: 2.);
    /// let array: PointArray<2> = vec![point].as_slice().into();
    /// let array_ref = array.as_ref();
    /// ```
    fn as_ref(&self) -> &dyn GeometryArrayTrait;

    // /// Clones this [`GeometryArray`] with a new new assigned bitmap.
    // /// # Panic
    // /// This function panics iff `validity.len() != self.len()`.
    // fn with_validity(&self, validity: Option<NullBuffer>) -> Box<dyn GeometryArray>;
}

/// Type alias for a dynamic reference to something that implements [GeometryArrayTrait].
pub type GeometryArrayRef = Arc<dyn GeometryArrayTrait>;

/// A trait for accessing the values of an [`Array`].
///
/// # Validity
///
/// An [`GeometryArrayAccessor`] must always return a well-defined value for an index that is
/// within the bounds `0..Array::len`, including for null indexes where [`Array::is_null`] is true.
///
/// The value at null indexes is unspecified, and implementations must not rely on a specific
/// value such as [`Default::default`] being returned, however, it must not be undefined.
pub trait GeometryArrayAccessor<'a>: GeometryArrayTrait {
    /// The [geoarrow scalar object][crate::scalar] for this geometry array type.
    type Item: Send + Sync + GeometryScalarTrait;

    /// The [`geo`] scalar object for this geometry array type.
    type ItemGeo: From<Self::Item>;

    /// Returns the element at index `i`, not considering validity.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::{trait_::GeometryArrayAccessor, array::PointArray, geo_traits::PointTrait};
    ///
    /// let point = geo::point!(x: 1., y: 2.);
    /// let array: PointArray<2> = vec![point].as_slice().into();
    /// let value = array.value(0); // geoarrow::scalar::Point<2>
    /// assert_eq!(value.x(), 1.);
    /// assert_eq!(value.y(), 2.);
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
    /// ```
    /// use geoarrow::{trait_::GeometryArrayAccessor, array::PointArray};
    ///
    /// let point = geo::point!(x: 1., y: 2.);
    /// let array: PointArray<2> = vec![point].as_slice().into();
    /// unsafe {
    ///     let value = array.value_unchecked(0); // geoarrow::scalar::Point<2>
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
    /// ```
    /// use geoarrow::{trait_::GeometryArrayAccessor, array::PointArray};
    ///
    /// let point = geo::point!(x: 1., y: 2.);
    /// let array: PointArray<2> = vec![point].as_slice().into();
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
    /// ```
    /// use geoarrow::{trait_::GeometryArrayAccessor, array::PointArray};
    ///
    /// let point = geo::point!(x: 1., y: 2.);
    /// let array: PointArray<2> = vec![point].as_slice().into();
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

    /// Returns the value at slot `i` as a [`geo`] scalar, not considering validity.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::{trait_::GeometryArrayAccessor, array::PointArray};
    ///
    /// let point = geo::point!(x: 1., y: 2.);
    /// let array: PointArray<2> = vec![point].as_slice().into();
    /// let value = array.value_as_geo(0); // geo::Point
    /// assert_eq!(value.x(), 1.);
    /// assert_eq!(value.y(), 2.);
    /// ```
    fn value_as_geo(&'a self, i: usize) -> Self::ItemGeo {
        self.value(i).into()
    }

    /// Returns the value at slot `i` as a [`geo`] scalar, considering validity.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::{trait_::GeometryArrayAccessor, array::PointArray};
    ///
    /// let point = geo::point!(x: 1., y: 2.);
    /// let array: PointArray<2> = vec![point].as_slice().into();
    /// assert!(array.get_as_geo(0).is_some());
    /// ```
    fn get_as_geo(&'a self, i: usize) -> Option<Self::ItemGeo> {
        if self.is_null(i) {
            return None;
        }

        Some(self.value_as_geo(i))
    }

    /// Iterates over this array's geoarrow scalar values, considering validity.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::{trait_::GeometryArrayAccessor, array::PointArray};
    ///
    /// let point = geo::point!(x: 1., y: 2.);
    /// let array: PointArray<2> = vec![point].as_slice().into();
    /// let maybe_points: Vec<Option<_>> = array.iter().collect();
    /// ```
    fn iter(&'a self) -> impl ExactSizeIterator<Item = Option<Self::Item>> + 'a {
        (0..self.len()).map(|i| unsafe { self.get_unchecked(i) })
    }

    /// Iterator over geoarrow scalar values, not considering validity.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::{trait_::GeometryArrayAccessor, array::PointArray};
    ///
    /// let point = geo::point!(x: 1., y: 2.);
    /// let array: PointArray<2> = vec![point].as_slice().into();
    /// let points: Vec<_> = array.iter_values().collect();
    /// ```
    fn iter_values(&'a self) -> impl ExactSizeIterator<Item = Self::Item> + 'a {
        (0..self.len()).map(|i| unsafe { self.value_unchecked(i) })
    }

    /// Iterator over geo scalar values, considering validity
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::{trait_::GeometryArrayAccessor, array::PointArray};
    ///
    /// let point = geo::point!(x: 1., y: 2.);
    /// let array: PointArray<2> = vec![point].as_slice().into();
    /// let maybe_points: Vec<Option<_>> = array.iter_geo().collect();
    /// ```
    fn iter_geo(&'a self) -> impl ExactSizeIterator<Item = Option<Self::ItemGeo>> + 'a {
        (0..self.len()).map(|i| unsafe { self.get_unchecked(i) }.map(|x| x.into()))
    }

    /// Iterator over geo scalar values, not looking at validity
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::{trait_::GeometryArrayAccessor, array::PointArray};
    ///
    /// let point = geo::point!(x: 1., y: 2.);
    /// let array: PointArray<2> = vec![point].as_slice().into();
    /// let points: Vec<_> = array.iter_geo_values().collect();
    /// ```
    fn iter_geo_values(&'a self) -> impl ExactSizeIterator<Item = Self::ItemGeo> + 'a {
        (0..self.len()).map(|i| unsafe { self.value_unchecked(i) }.into())
    }
}

/// Trait for geometry array methods that return `Self`.
///
/// TODO Horrible name, to be changed to a better name in the future!!
pub trait GeometryArraySelfMethods<const D: usize> {
    /// Creates a new array with replaced coordinates.
    ///
    /// This is useful if you want to apply an operation to _every_ coordinate in unison, such as a
    /// reprojection or a scaling operation, with no regards to each individual geometry
    ///
    /// # Example
    ///
    /// ```
    /// use geoarrow::{
    ///     array::{PointArray, CoordBuffer, InterleavedCoordBuffer},
    ///     trait_::{GeometryArraySelfMethods, GeometryArrayAccessor},
    /// };
    ///
    /// let point = geo::point!(x: 1., y: 2.);
    /// let array: PointArray<2> = vec![point].as_slice().into();
    /// let coords = CoordBuffer::Interleaved(InterleavedCoordBuffer::new(vec![3., 4.].into()));
    /// let array = array.with_coords(coords);
    /// let value = array.value_as_geo(0);
    /// assert_eq!(value.x(), 3.);
    /// assert_eq!(value.y(), 4.);
    /// ```
    fn with_coords(self, coords: CoordBuffer<D>) -> Self;

    /// Casts the coordinate buffer of this geometry array to the given coordinate type.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::{
    ///     array::{PointArray, CoordType, CoordBuffer},
    ///     trait_::{GeometryArrayAccessor, GeometryArraySelfMethods},
    /// };
    ///
    /// let point_0 = geo::point!(x: 1., y: 2.);
    /// let point_1 = geo::point!(x: 3., y: 4.);
    /// let array_interleaved: PointArray<2> = vec![point_0, point_1].as_slice().into();
    /// let array_separated = array_interleaved.into_coord_type(CoordType::Separated);
    /// assert!(matches!(array_separated.coords(), &CoordBuffer::Separated(_)));
    /// ```
    fn into_coord_type(self, coord_type: CoordType) -> Self;

    /// Returns a zero-copy slice of this array with the indicated offset and length.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::{
    ///     array::PointArray,
    ///     trait_::{GeometryArraySelfMethods, GeometryArrayAccessor, GeometryArrayTrait}
    /// };
    ///
    /// let point_0 = geo::point!(x: 1., y: 2.);
    /// let point_1 = geo::point!(x: 3., y: 4.);
    /// let array: PointArray<2> = vec![point_0, point_1].as_slice().into();
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
    fn slice(&self, offset: usize, length: usize) -> Self;

    /// Returns a owned slice that fully copies the contents of the underlying buffer.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::{array::PointArray, trait_::GeometryArraySelfMethods};
    ///
    /// let point_0 = geo::point!(x: 1., y: 2.);
    /// let point_1 = geo::point!(x: 3., y: 4.);
    /// let array: PointArray<2> = vec![point_0, point_1].as_slice().into();
    /// let smaller_array = array.owned_slice(1, 1);
    /// ```
    #[must_use]
    fn owned_slice(&self, offset: usize, length: usize) -> Self;
}

/// Convert GeoArrow arrays into their underlying arrow arrays.
pub trait IntoArrow {
    /// The type of arrow array that this geoarrow array can be converted into.
    type ArrowArray;

    /// Converts this geoarrow array into an arrow array.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::{trait_::IntoArrow, array::PointArray};
    ///
    /// let point = geo::point!(x: 1., y: 2.);
    /// let array: PointArray<2> = vec![point].as_slice().into();
    /// let arrow_array = array.into_arrow();
    /// ```
    fn into_arrow(self) -> Self::ArrowArray;
}

/// A trait for converting geoarrow scalar types to their [mod@geo] equivalent.
pub trait GeometryScalarTrait {
    /// The [`geo`] scalar object for this geometry array type.
    type ScalarGeo;

    /// Converts this value to its [mod@geo] equivalent.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::{trait_::{GeometryScalarTrait, GeometryArrayAccessor}, array::PointArray};
    ///
    /// let point = geo::point!(x: 1., y: 2.);
    /// let array: PointArray<2> = vec![point].as_slice().into();
    /// let point = array.value(0).to_geo(); // array.value_as_geo(0) does the same thing
    /// assert_eq!(point.x(), 1.);
    /// assert_eq!(point.y(), 2.);
    /// ```
    fn to_geo(&self) -> Self::ScalarGeo;

    /// Converts this value to a [geo::Geometry].
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::{trait_::{GeometryScalarTrait, GeometryArrayAccessor}, array::PointArray};
    ///
    /// let point = geo::point!(x: 1., y: 2.);
    /// let array: PointArray<2> = vec![point].as_slice().into();
    /// let geometry = array.value(0).to_geo_geometry();
    /// ```
    fn to_geo_geometry(&self) -> geo::Geometry;

    /// Converts this value to a [geos::Geometry].
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::{trait_::{GeometryScalarTrait, GeometryArrayAccessor}, array::PointArray};
    ///
    /// let point = geo::point!(x: 1., y: 2.);
    /// let array: PointArray<2> = vec![point].as_slice().into();
    /// let geometry = array.value(0).to_geos().unwrap();
    /// ```
    #[cfg(feature = "geos")]
    fn to_geos(&self) -> std::result::Result<geos::Geometry, geos::Error>;
}

/// A trait describing a mutable geometry array; i.e. an array whose values can be changed.
///
/// Mutable arrays cannot be cloned but can be mutated in place,
/// thereby making them useful to perform numeric operations without allocations.
/// As in [`GeometryArrayTrait`], concrete arrays (such as
/// [`PointBuilder`][crate::array::PointBuilder]) implement how they are mutated.
pub trait GeometryArrayBuilder: std::fmt::Debug + Send + Sync + Sized {
    /// Returns the length of the array.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::{array::PointBuilder, trait_::GeometryArrayBuilder};
    ///
    /// let mut builder = PointBuilder::new();
    /// assert_eq!(builder.len(), 0);
    /// builder.push_point(Some(&geo::point!(x: 1., y: 2.)));
    /// assert_eq!(builder.len(), 1);
    /// ```
    fn len(&self) -> usize;

    /// Returns whether the array is empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::{array::PointBuilder, trait_::GeometryArrayBuilder};
    ///
    /// let mut builder = PointBuilder::new();
    /// assert!(builder.is_empty());
    /// builder.push_point(Some(&geo::point!(x: 1., y: 2.)));
    /// assert!(!builder.is_empty());
    /// ```
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the validity buffer of this array.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::{array::PointBuilder, trait_::GeometryArrayBuilder};
    ///
    /// let builder = PointBuilder::<2>::new();
    /// assert!(builder.nulls().is_empty());
    /// ```
    fn nulls(&self) -> &NullBufferBuilder;

    /// Creates a new builder.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::{array::PointBuilder, trait_::GeometryArrayBuilder};
    /// let builder = PointBuilder::<2>::new();
    /// ```
    fn new() -> Self;

    /// Creates a new builder with capacity and other options.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::{
    ///     array::{PointBuilder, CoordType, metadata::{ArrayMetadata, Edges}},
    ///     trait_::GeometryArrayBuilder,
    /// };
    /// let metadata = ArrayMetadata {
    ///     crs: None,
    ///     edges: Some(Edges::Spherical),
    /// };
    /// let builder = PointBuilder::<2>::with_geom_capacity_and_options(
    ///     2,
    ///     CoordType::Interleaved,
    ///     metadata.into()
    /// );
    /// ```
    fn with_geom_capacity_and_options(
        geom_capacity: usize,
        coord_type: CoordType,
        metadata: Arc<ArrayMetadata>,
    ) -> Self;

    /// Creates a new builder with the given capacity.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::{
    ///     array::PointBuilder,
    ///     trait_::GeometryArrayBuilder,
    /// };
    /// let builder = PointBuilder::<2>::with_geom_capacity(2);
    /// ```
    fn with_geom_capacity(geom_capacity: usize) -> Self {
        GeometryArrayBuilder::with_geom_capacity_and_options(
            geom_capacity,
            Default::default(),
            Default::default(),
        )
    }

    /// Sets this builders metadata.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::{
    ///     array::{PointBuilder, metadata::{ArrayMetadata, Edges}},
    ///     trait_::GeometryArrayBuilder,
    /// };
    /// let mut builder = PointBuilder::<2>::new();
    /// let metadata = ArrayMetadata {
    ///     crs: None,
    ///     edges: Some(Edges::Spherical),
    /// };
    /// builder.set_metadata(metadata.into());
    /// ```
    fn set_metadata(&mut self, metadata: Arc<ArrayMetadata>);

    /// Finishes building the underlying data structures and returns a geometry array.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::{array::PointBuilder, trait_::{GeometryArrayBuilder, GeometryArrayTrait}};
    ///
    /// let mut builder = PointBuilder::new();
    /// builder.push_point(Some(&geo::point!(x: 1., y: 2.)));
    /// let array = builder.finish();
    /// assert_eq!(array.len(), 1);
    /// ```
    fn finish(self) -> Arc<dyn GeometryArrayTrait>;

    /// Returns the [CoordType] of this geometry array.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::{array::{PointBuilder, CoordType}, trait_::GeometryArrayBuilder};
    /// let builder = PointBuilder::<2>::new();
    /// assert_eq!(builder.coord_type(), CoordType::Interleaved);
    /// ```
    fn coord_type(&self) -> CoordType;

    /// Returns the [ArrayMetadata] of this geometry array.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::{array::{PointBuilder, CoordType}, trait_::GeometryArrayBuilder};
    /// let builder = PointBuilder::<2>::new();
    /// let metadata = builder.metadata();
    /// ```
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

    /// Converts this builder into a dynamic array reference.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::{array::PointBuilder, trait_::GeometryArrayBuilder};
    /// let builder = PointBuilder::<2>::new();
    /// let array_ref = builder.into_array_ref();
    /// ```
    fn into_array_ref(self) -> Arc<dyn Array>;
}
