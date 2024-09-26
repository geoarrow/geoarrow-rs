//! Contains implementations of _chunked_ GeoArrow arrays.
//!
//! In contrast to the structures in [array](crate::array), these data structures only have contiguous
//! memory within each individual _chunk_. These chunked arrays are essentially wrappers around a
//! [Vec] of geometry arrays.
//!
//! Additionally, if the `rayon` feature is active, operations on chunked arrays will automatically
//! be parallelized across each chunk.

#[allow(missing_docs)] // FIXME
mod dynamic;

pub use dynamic::ChunkedNativeArrayDyn;

use std::any::Any;
use std::sync::Arc;

use arrow::array::OffsetSizeTrait;
use arrow_array::{make_array, Array, ArrayRef};
use arrow_schema::{DataType, Field};

#[cfg(feature = "rayon")]
use rayon::prelude::*;

use crate::array::*;
use crate::datatypes::NativeType;
use crate::error::{GeoArrowError, Result};
use crate::trait_::{ArrayAccessor, NativeArrayRef};
use crate::NativeArray;

/// A collection of Arrow arrays of the same type.
///
/// This can be thought of as a column in a table, as Table objects normally have internal batches.
#[derive(Debug, Clone, PartialEq)]
pub struct ChunkedArray<A: Array> {
    pub(crate) chunks: Vec<A>,
    length: usize,
}

impl<A: Array> ChunkedArray<A> {
    /// Creates a new chunked array from multiple arrays.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::chunked_array::ChunkedArray;
    /// use arrow_array::Int32Array;
    ///
    /// let array_0 = Int32Array::from(vec![1, 2]);
    /// let array_1 = Int32Array::from(vec![3, 4]);
    /// let chunked_array = ChunkedArray::new(vec![array_0, array_1]);
    /// ```
    pub fn new(chunks: Vec<A>) -> Self {
        let mut length = 0;
        chunks.iter().for_each(|x| length += x.len());
        if !chunks
            .windows(2)
            .all(|w| w[0].data_type() == w[1].data_type())
        {
            // TODO: switch to try_new with Err
            panic!("All data types should be the same.")
        }

        Self { chunks, length }
    }

    /// Converts this chunked array into its inner chunks.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::chunked_array::ChunkedArray;
    /// use arrow_array::Int32Array;
    ///
    /// let array_0 = Int32Array::from(vec![1, 2]);
    /// let array_1 = Int32Array::from(vec![3, 4]);
    /// let chunked_array = ChunkedArray::new(vec![array_0, array_1]);
    /// let chunks = chunked_array.into_inner();
    /// assert_eq!(chunks.len(), 2);
    /// ```
    pub fn into_inner(self) -> Vec<A> {
        self.chunks
    }

    /// Returns this chunked array's length.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::chunked_array::ChunkedArray;
    /// use arrow_array::Int32Array;
    ///
    /// let array_0 = Int32Array::from(vec![1, 2]);
    /// let array_1 = Int32Array::from(vec![3, 4]);
    /// let chunked_array = ChunkedArray::new(vec![array_0, array_1]);
    /// assert_eq!(chunked_array.len(), 4);
    /// ```
    pub fn len(&self) -> usize {
        self.length
    }

    /// Returns true if chunked array is empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::chunked_array::ChunkedArray;
    /// use arrow_array::Int32Array;
    ///
    /// assert!(ChunkedArray::<Int32Array>::new(Vec::new()).is_empty());
    ///
    /// let array_0 = Int32Array::from(vec![1, 2]);
    /// let array_1 = Int32Array::from(vec![3, 4]);
    /// let chunked_array = ChunkedArray::new(vec![array_0, array_1]);
    /// assert!(!chunked_array.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns this chunked array's data type.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::chunked_array::ChunkedArray;
    /// use arrow_array::Int32Array;
    /// use arrow_schema::DataType;
    ///
    /// let array_0 = Int32Array::from(vec![1, 2]);
    /// let array_1 = Int32Array::from(vec![3, 4]);
    /// let chunked_array = ChunkedArray::new(vec![array_0, array_1]);
    /// assert_eq!(chunked_array.data_type(), &DataType::Int32);
    /// ```
    pub fn data_type(&self) -> &DataType {
        self.chunks.first().unwrap().data_type()
    }

    /// Returns the number of nulls in this chunked array.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::chunked_array::ChunkedArray;
    /// use arrow_array::Int32Array;
    ///
    /// let array_0 = Int32Array::from(vec![1, 2]);
    /// let array_1 = Int32Array::from(vec![3, 4]);
    /// let chunked_array = ChunkedArray::new(vec![array_0, array_1]);
    /// assert_eq!(chunked_array.null_count(), 0);
    /// ```
    pub fn null_count(&self) -> usize {
        self.chunks()
            .iter()
            .fold(0, |acc, chunk| acc + chunk.null_count())
    }

    /// Returns an immutable reference to this chunked array's chunks.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::chunked_array::ChunkedArray;
    /// use arrow_array::Int32Array;
    ///
    /// let array_0 = Int32Array::from(vec![1, 2]);
    /// let array_1 = Int32Array::from(vec![3, 4]);
    /// let chunked_array = ChunkedArray::new(vec![array_0, array_1]);
    /// let chunks = chunked_array.chunks();
    /// ```
    pub fn chunks(&self) -> &[A] {
        self.chunks.as_slice()
    }

    /// Returns a Vec of dynamically-typed [ArrayRef].
    pub fn chunk_refs(&self) -> Vec<ArrayRef> {
        self.chunks
            .iter()
            .map(|arr| make_array(arr.to_data()))
            .collect()
    }

    /// Applies an operation over each chunk of this chunked array.
    ///
    /// If the `rayon` feature is enabled, this will be done in parallel.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::chunked_array::ChunkedArray;
    /// use arrow_array::Int32Array;
    ///
    /// let array_0 = Int32Array::from(vec![1]);
    /// let array_1 = Int32Array::from(vec![3, 4]);
    /// let chunked_array = ChunkedArray::new(vec![array_0, array_1]);
    /// let lengths = chunked_array.map(|chunk| chunk.len());
    /// assert_eq!(lengths, vec![1, 2]);
    /// ```
    #[allow(dead_code)]
    pub fn map<F: Fn(&A) -> R + Sync + Send, R: Send>(&self, map_op: F) -> Vec<R> {
        #[cfg(feature = "rayon")]
        {
            let mut output_vec = Vec::with_capacity(self.chunks.len());
            self.chunks
                .par_iter()
                .map(map_op)
                .collect_into_vec(&mut output_vec);
            output_vec
        }

        #[cfg(not(feature = "rayon"))]
        {
            self.chunks.iter().map(map_op).collect()
        }
    }
    /// Applies an operation over each chunk of this chunked array, returning a `Result`.
    ///
    /// If the `rayon` feature is enabled, this will be done in parallel.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::chunked_array::ChunkedArray;
    /// use arrow_array::Int32Array;
    ///
    /// let array_0 = Int32Array::from(vec![1]);
    /// let array_1 = Int32Array::from(vec![3, 4]);
    /// let chunked_array = ChunkedArray::new(vec![array_0, array_1]);
    /// let lengths = chunked_array.try_map(|chunk| Ok(chunk.len())).unwrap();
    /// assert_eq!(lengths, vec![1, 2]);
    /// ```
    pub fn try_map<F: Fn(&A) -> Result<R> + Sync + Send, R: Send>(
        &self,
        map_op: F,
    ) -> Result<Vec<R>> {
        #[cfg(feature = "rayon")]
        {
            self.chunks.par_iter().map(map_op).collect()
        }

        #[cfg(not(feature = "rayon"))]
        {
            self.chunks.iter().map(map_op).collect()
        }
    }
}

impl<A: Array> TryFrom<Vec<A>> for ChunkedArray<A> {
    type Error = GeoArrowError;

    fn try_from(value: Vec<A>) -> Result<Self> {
        Ok(Self::new(value))
    }
}

impl<A: Array> AsRef<[A]> for ChunkedArray<A> {
    fn as_ref(&self) -> &[A] {
        &self.chunks
    }
}

/// A collection of GeoArrow geometry arrays of the same type.
///
/// This can be thought of as a geometry column in a table, as Table objects normally have internal
/// batches.
///
/// # Invariants
///
/// Must have at least one chunk.
#[derive(Debug, Clone, PartialEq)]
pub struct ChunkedGeometryArray<G: ArrayBase> {
    pub(crate) chunks: Vec<G>,
    length: usize,
}

impl<G: ArrayBase> ChunkedGeometryArray<G> {
    /// Creates a new chunked geometry array from multiple arrays.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::{chunked_array::ChunkedGeometryArray, array::PointArray};
    ///
    /// let array_0: PointArray<2> = vec![&geo::point!(x: 1., y: 2.)].as_slice().into();
    /// let array_1: PointArray<2> = vec![&geo::point!(x: 3., y: 4.)].as_slice().into();
    /// let chunked_array = ChunkedGeometryArray::new(vec![array_0, array_1]);
    /// ```
    pub fn new(chunks: Vec<G>) -> Self {
        // TODO: assert all equal extension fields
        let mut length = 0;
        chunks.iter().for_each(|x| length += x.len());
        Self { chunks, length }
    }

    /// Returns the extension field for this chunked geometry array.
    ///
    /// TODO: check/assert on creation that all are the same so we can be comfortable here only
    /// taking the first.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::{chunked_array::ChunkedGeometryArray, array::PointArray};
    ///
    /// let array_0: PointArray<2> = vec![&geo::point!(x: 1., y: 2.)].as_slice().into();
    /// let array_1: PointArray<2> = vec![&geo::point!(x: 3., y: 4.)].as_slice().into();
    /// let chunked_array = ChunkedGeometryArray::new(vec![array_0, array_1]);
    /// let field = chunked_array.extension_field();
    /// assert_eq!(field.name(), "geometry");
    /// ```
    pub fn extension_field(&self) -> Arc<Field> {
        self.chunks.first().unwrap().extension_field()
    }

    /// Converts this chunked geometry array into its inner chunks.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::{chunked_array::ChunkedGeometryArray, array::PointArray};
    ///
    /// let array_0: PointArray<2> = vec![&geo::point!(x: 1., y: 2.)].as_slice().into();
    /// let array_1: PointArray<2> = vec![&geo::point!(x: 3., y: 4.)].as_slice().into();
    /// let chunked_array = ChunkedGeometryArray::new(vec![array_0, array_1]);
    /// let chunks = chunked_array.into_inner();
    /// assert_eq!(chunks.len(), 2);
    /// ```
    pub fn into_inner(self) -> Vec<G> {
        self.chunks
    }

    /// Returns this chunked geometry array length.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::{chunked_array::ChunkedGeometryArray, array::PointArray};
    ///
    /// let array_0: PointArray<2> = vec![&geo::point!(x: 1., y: 2.)].as_slice().into();
    /// let array_1: PointArray<2> = vec![&geo::point!(x: 3., y: 4.), &geo::point!(x: 5., y: 6.)].as_slice().into();
    /// let chunked_array = ChunkedGeometryArray::new(vec![array_0, array_1]);
    /// assert_eq!(chunked_array.len(), 3);
    /// ```
    pub fn len(&self) -> usize {
        self.length
    }

    /// Returns true if this chunked geometry array is empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::{chunked_array::ChunkedGeometryArray, array::PointArray};
    ///
    /// let array_0: PointArray<2> = vec![&geo::point!(x: 1., y: 2.)].as_slice().into();
    /// let array_1: PointArray<2> = vec![&geo::point!(x: 3., y: 4.)].as_slice().into();
    /// let chunked_array = ChunkedGeometryArray::new(vec![array_0, array_1]);
    /// assert!(!chunked_array.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns an immutable reference to this array's chunks.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::{chunked_array::ChunkedGeometryArray, array::PointArray};
    ///
    /// let array_0: PointArray<2> = vec![&geo::point!(x: 1., y: 2.)].as_slice().into();
    /// let array_1: PointArray<2> = vec![&geo::point!(x: 3., y: 4.)].as_slice().into();
    /// let chunked_array = ChunkedGeometryArray::new(vec![array_0, array_1]);
    /// let chunks = chunked_array.chunks();
    /// ```
    pub fn chunks(&self) -> &[G] {
        self.chunks.as_slice()
    }

    /// Converts this chunked array into a vector, where each element is the output of `map_op` for one chunk.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::{
    ///     chunked_array::ChunkedGeometryArray,
    ///     array::PointArray,
    ///     trait_::ArrayBase,
    ///     datatypes::NativeType,
    /// };
    ///
    /// let array_0: PointArray<2> = vec![&geo::point!(x: 1., y: 2.)].as_slice().into();
    /// let array_1: PointArray<2> = vec![&geo::point!(x: 3., y: 4.)].as_slice().into();
    /// let chunked_array = ChunkedGeometryArray::new(vec![array_0, array_1]);
    /// let lengths = chunked_array.into_map(|chunk| chunk.len()); // chunked_array is consumed
    /// assert_eq!(lengths, vec![1, 1]);
    /// ```
    pub fn into_map<F: Fn(G) -> R + Sync + Send, R: Send>(self, map_op: F) -> Vec<R> {
        #[cfg(feature = "rayon")]
        {
            let mut output_vec = Vec::with_capacity(self.chunks.len());
            self.chunks
                .into_par_iter()
                .map(map_op)
                .collect_into_vec(&mut output_vec);
            output_vec
        }

        #[cfg(not(feature = "rayon"))]
        {
            self.chunks.into_iter().map(map_op).collect()
        }
    }

    /// Maps this chunked array into a vector, where each element is the output of `map_op` for one chunk.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::{
    ///     chunked_array::ChunkedGeometryArray,
    ///     array::PointArray,
    ///     trait_::ArrayBase,
    ///     datatypes::NativeType,
    /// };
    ///
    /// let array_0: PointArray<2> = vec![&geo::point!(x: 1., y: 2.)].as_slice().into();
    /// let array_1: PointArray<2> = vec![&geo::point!(x: 3., y: 4.)].as_slice().into();
    /// let chunked_array = ChunkedGeometryArray::new(vec![array_0, array_1]);
    /// let lengths = chunked_array.map(|chunk| chunk.len());
    /// assert_eq!(lengths, vec![1, 1]);
    /// ```
    pub fn map<F: Fn(&G) -> R + Sync + Send, R: Send>(&self, map_op: F) -> Vec<R> {
        #[cfg(feature = "rayon")]
        {
            let mut output_vec = Vec::with_capacity(self.chunks.len());
            self.chunks
                .par_iter()
                .map(map_op)
                .collect_into_vec(&mut output_vec);
            output_vec
        }

        #[cfg(not(feature = "rayon"))]
        {
            self.chunks.iter().map(map_op).collect()
        }
    }

    /// Maps this chunked array into a vector, where each element is the `Result` output of `map_op` for one chunk.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::{
    ///     chunked_array::ChunkedGeometryArray,
    ///     array::PointArray,
    ///     trait_::ArrayBase,
    ///     datatypes::NativeType,
    /// };
    ///
    /// let array_0: PointArray<2> = vec![&geo::point!(x: 1., y: 2.)].as_slice().into();
    /// let array_1: PointArray<2> = vec![&geo::point!(x: 3., y: 4.)].as_slice().into();
    /// let chunked_array = ChunkedGeometryArray::new(vec![array_0, array_1]);
    /// let lengths = chunked_array.try_map(|chunk| Ok(chunk.len())).unwrap();
    /// assert_eq!(lengths, vec![1, 1]);
    /// ```
    pub fn try_map<F: Fn(&G) -> Result<R> + Sync + Send, R: Send>(
        &self,
        map_op: F,
    ) -> Result<Vec<R>> {
        #[cfg(feature = "rayon")]
        {
            self.chunks.par_iter().map(map_op).collect()
        }

        #[cfg(not(feature = "rayon"))]
        {
            self.chunks.iter().map(map_op).collect()
        }
    }
}

impl<G: NativeArray> ChunkedGeometryArray<G> {
    /// Returns this array's geo data type.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::{chunked_array::ChunkedGeometryArray, array::PointArray, datatypes::NativeType};
    ///
    /// let array_0: PointArray<2> = vec![&geo::point!(x: 1., y: 2.)].as_slice().into();
    /// let array_1: PointArray<2> = vec![&geo::point!(x: 3., y: 4.)].as_slice().into();
    /// let chunked_array = ChunkedGeometryArray::new(vec![array_0, array_1]);
    /// assert!(matches!(chunked_array.data_type(), NativeType::Point(_, _)));
    /// ```
    pub fn data_type(&self) -> NativeType {
        self.chunks.first().unwrap().data_type()
    }
}

impl<'a, G: NativeArray + ArrayAccessor<'a>> ChunkedGeometryArray<G> {
    /// Returns a value from this chunked array, ignoring validity.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::{chunked_array::ChunkedGeometryArray, array::PointArray};
    ///
    /// let array_0: PointArray<2> = vec![&geo::point!(x: 1., y: 2.)].as_slice().into();
    /// let array_1: PointArray<2> = vec![&geo::point!(x: 3., y: 4.)].as_slice().into();
    /// let chunked_array = ChunkedGeometryArray::new(vec![array_0, array_1]);
    /// let value = chunked_array.value(1); // geoarrow::scalar::Point<2>
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if the index exceeds the size of this chunked array.
    pub fn value(&'a self, index: usize) -> G::Item {
        assert!(index <= self.len());
        let mut index = index;
        for chunk in self.chunks() {
            if index >= chunk.len() {
                index -= chunk.len();
            } else {
                return chunk.value(index);
            }
        }
        unreachable!()
    }

    /// Returns a value from this chunked array, considering validity.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::{chunked_array::ChunkedGeometryArray, array::PointArray};
    ///
    /// let array_0: PointArray<2> = vec![&geo::point!(x: 1., y: 2.)].as_slice().into();
    /// let array_1: PointArray<2> = vec![&geo::point!(x: 3., y: 4.)].as_slice().into();
    /// let chunked_array = ChunkedGeometryArray::new(vec![array_0, array_1]);
    /// let value = chunked_array.get(1).unwrap(); // geoarrow::scalar::Point<2>
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if the index exceeds the size of this chunked array.
    pub fn get(&'a self, index: usize) -> Option<G::Item> {
        assert!(index <= self.len());
        let mut index = index;
        for chunk in self.chunks() {
            if index >= chunk.len() {
                index -= chunk.len();
            } else {
                return chunk.get(index);
            }
        }
        unreachable!()
    }
}

impl<G: ArrayBase> TryFrom<Vec<G>> for ChunkedGeometryArray<G> {
    type Error = GeoArrowError;

    fn try_from(value: Vec<G>) -> Result<Self> {
        Ok(Self::new(value))
    }
}

/// A chunked point array.
pub type ChunkedPointArray<const D: usize> = ChunkedGeometryArray<PointArray<D>>;
/// A chunked line string array.
pub type ChunkedLineStringArray<O, const D: usize> = ChunkedGeometryArray<LineStringArray<O, D>>;
/// A chunked polygon array.
pub type ChunkedPolygonArray<O, const D: usize> = ChunkedGeometryArray<PolygonArray<O, D>>;
/// A chunked multi-point array.
pub type ChunkedMultiPointArray<O, const D: usize> = ChunkedGeometryArray<MultiPointArray<O, D>>;
/// A chunked mutli-line string array.
pub type ChunkedMultiLineStringArray<O, const D: usize> =
    ChunkedGeometryArray<MultiLineStringArray<O, D>>;
/// A chunked multi-polygon array.
pub type ChunkedMultiPolygonArray<O, const D: usize> =
    ChunkedGeometryArray<MultiPolygonArray<O, D>>;
/// A chunked mixed geometry array.
pub type ChunkedMixedGeometryArray<O, const D: usize> =
    ChunkedGeometryArray<MixedGeometryArray<O, D>>;
/// A chunked geometry collection array.
pub type ChunkedGeometryCollectionArray<O, const D: usize> =
    ChunkedGeometryArray<GeometryCollectionArray<O, D>>;
/// A chunked WKB array.
pub type ChunkedWKBArray<O> = ChunkedGeometryArray<WKBArray<O>>;
/// A chunked rect array.
pub type ChunkedRectArray<const D: usize> = ChunkedGeometryArray<RectArray<D>>;
/// A chunked unknown geometry array.
#[allow(dead_code)]
pub type ChunkedUnknownGeometryArray = ChunkedGeometryArray<Arc<dyn NativeArray>>;

/// A base chunked array trait that applies to all GeoArrow arrays, both "native" and "serialized"
/// encodings.
pub trait ChunkedArrayBase: std::fmt::Debug + Send + Sync {
    /// Returns the array as [`Any`] so that it can be downcasted to a specific implementation.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::{
    ///     chunked_array::{ChunkedGeometryArray, ChunkedNativeArray, ChunkedArrayBase},
    ///     array::PointArray
    /// };
    ///
    /// let array_0: PointArray<2> = vec![&geo::point!(x: 1., y: 2.)].as_slice().into();
    /// let array_1: PointArray<2> = vec![&geo::point!(x: 3., y: 4.)].as_slice().into();
    /// let chunked_array = ChunkedGeometryArray::new(vec![array_0, array_1]);
    /// let any = chunked_array.as_any();
    /// ```
    fn as_any(&self) -> &dyn Any;

    /// Returns an Arrow [`Field`] describing this chunked array.
    ///
    /// This field will always have the `ARROW:extension:name` key of the field
    /// metadata set, signifying that it describes a GeoArrow extension type.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::{
    ///     chunked_array::{ChunkedGeometryArray, ChunkedNativeArray},
    ///     array::PointArray
    /// };
    ///
    /// let array_0: PointArray<2> = vec![&geo::point!(x: 1., y: 2.)].as_slice().into();
    /// let array_1: PointArray<2> = vec![&geo::point!(x: 3., y: 4.)].as_slice().into();
    /// let chunked_array = ChunkedGeometryArray::new(vec![array_0, array_1]);
    /// let field = chunked_array.extension_field();
    /// assert_eq!(field.metadata()["ARROW:extension:name"], "geoarrow.point");
    /// ```
    fn extension_field(&self) -> Arc<Field>;

    /// The number of geometries contained in this array.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::{array::PointArray, NativeArray, ArrayBase};
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
    /// use geoarrow::{array::PointArray, ArrayBase};
    ///
    /// let point = geo::point!(x: 1., y: 2.);
    /// let point_array: PointArray<2> = vec![point].as_slice().into();
    /// assert!(!point_array.is_empty());
    /// ```
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the number of chunks in this chunked array.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::{
    ///     chunked_array::{ChunkedGeometryArray, ChunkedArrayBase},
    ///     array::PointArray
    /// };
    ///
    /// let array_0: PointArray<2> = vec![&geo::point!(x: 1., y: 2.)].as_slice().into();
    /// let array_1: PointArray<2> = vec![&geo::point!(x: 3., y: 4.)].as_slice().into();
    /// let chunked_array = ChunkedGeometryArray::new(vec![array_0, array_1]);
    /// assert_eq!(chunked_array.num_chunks(), 2);
    /// ```
    fn num_chunks(&self) -> usize;

    /// Returns a vector of references to the underlying arrow arrays.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::{
    ///     chunked_array::{ChunkedGeometryArray, ChunkedArrayBase},
    ///     array::PointArray
    /// };
    ///
    /// let array_0: PointArray<2> = vec![&geo::point!(x: 1., y: 2.)].as_slice().into();
    /// let array_1: PointArray<2> = vec![&geo::point!(x: 3., y: 4.)].as_slice().into();
    /// let chunked_array = ChunkedGeometryArray::new(vec![array_0, array_1]);
    /// let arrays = chunked_array.array_refs();
    /// ```
    fn array_refs(&self) -> Vec<Arc<dyn Array>>;
}

/// A trait implemented by all chunked geometry arrays.
///
/// This trait is often used for downcasting. For example, the [`from_geoarrow_chunks`] function
/// returns a dynamically-typed `Arc<dyn ChunkedNativeArray>`. To downcast into a
/// strongly-typed chunked array, use `as_any` with the `data_type` method to discern which chunked
/// array type to pass to `downcast_ref`.
pub trait ChunkedNativeArray: ChunkedArrayBase {
    /// Returns a reference to the [`NativeType`] of this array.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::{
    ///     chunked_array::{ChunkedGeometryArray, ChunkedNativeArray},
    ///     array::PointArray
    /// };
    ///
    /// let array_0: PointArray<2> = vec![&geo::point!(x: 1., y: 2.)].as_slice().into();
    /// let array_1: PointArray<2> = vec![&geo::point!(x: 3., y: 4.)].as_slice().into();
    /// let chunked_array = ChunkedGeometryArray::new(vec![array_0, array_1]);
    /// let data_type = chunked_array.data_type();
    /// ```
    fn data_type(&self) -> NativeType;

    /// Returns a vector of references to the geometry chunks contained within this chunked array.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::{
    ///     chunked_array::{ChunkedGeometryArray, ChunkedNativeArray},
    ///     array::PointArray
    /// };
    ///
    /// let array_0: PointArray<2> = vec![&geo::point!(x: 1., y: 2.)].as_slice().into();
    /// let array_1: PointArray<2> = vec![&geo::point!(x: 3., y: 4.)].as_slice().into();
    /// let chunked_array = ChunkedGeometryArray::new(vec![array_0, array_1]);
    /// let chunks = chunked_array.geometry_chunks();
    /// assert_eq!(chunks.len(), 2);
    /// ```
    fn geometry_chunks(&self) -> Vec<Arc<dyn NativeArray>>;

    /// Returns a reference to this chunked geometry array.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::{
    ///     chunked_array::{ChunkedGeometryArray, ChunkedNativeArray},
    ///     array::PointArray
    /// };
    ///
    /// let array_0: PointArray<2> = vec![&geo::point!(x: 1., y: 2.)].as_slice().into();
    /// let array_1: PointArray<2> = vec![&geo::point!(x: 3., y: 4.)].as_slice().into();
    /// let chunked_array = ChunkedGeometryArray::new(vec![array_0, array_1]);
    /// let array_ref = chunked_array.as_ref();
    /// ```
    fn as_ref(&self) -> &dyn ChunkedNativeArray;

    /// Returns a zero-copy slice of this array with the indicated offset and length.
    fn slice(&self, mut offset: usize, mut length: usize) -> Result<Arc<dyn ChunkedNativeArray>> {
        if offset + length > self.len() {
            panic!("offset + length may not exceed length of array")
        }

        let mut sliced_chunks: Vec<NativeArrayRef> = vec![];
        for chunk in self.geometry_chunks() {
            if chunk.is_empty() {
                continue;
            }

            // If the offset is greater than the len of this chunk, don't include any rows from
            // this chunk
            if offset >= chunk.len() {
                offset -= chunk.len();
                continue;
            }

            let take_count = length.min(chunk.len() - offset);
            let sliced_chunk = chunk.slice(offset, take_count);
            sliced_chunks.push(sliced_chunk);

            length -= take_count;

            // If we've selected all rows, exit
            if length == 0 {
                break;
            } else {
                offset = 0;
            }
        }

        let refs = sliced_chunks.iter().map(|x| x.as_ref()).collect::<Vec<_>>();
        Ok(ChunkedNativeArrayDyn::from_geoarrow_chunks(refs.as_slice())?.into_inner())
    }
}

impl<const D: usize> ChunkedArrayBase for ChunkedPointArray<D> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    // TODO: check/assert on creation that all are the same so we can be comfortable here only
    // taking the first.
    fn extension_field(&self) -> Arc<Field> {
        self.chunks.first().unwrap().extension_field()
    }

    fn len(&self) -> usize {
        self.len()
    }
    fn num_chunks(&self) -> usize {
        self.chunks.len()
    }

    fn array_refs(&self) -> Vec<Arc<dyn Array>> {
        self.chunks
            .iter()
            .map(|chunk| chunk.to_array_ref())
            .collect()
    }
}

impl<const D: usize> ChunkedNativeArray for ChunkedPointArray<D> {
    fn data_type(&self) -> NativeType {
        self.chunks.first().unwrap().data_type()
    }

    fn geometry_chunks(&self) -> Vec<Arc<dyn NativeArray>> {
        self.chunks
            .iter()
            .map(|chunk| Arc::new(chunk.clone()) as NativeArrayRef)
            .collect()
    }

    fn as_ref(&self) -> &dyn ChunkedNativeArray {
        self
    }
}

impl<O: OffsetSizeTrait> ChunkedArrayBase for ChunkedWKBArray<O> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    // fn data_type(&self) -> NativeType {
    //     self.chunks.first().unwrap().data_type()
    // }

    // TODO: check/assert on creation that all are the same so we can be comfortable here only
    // taking the first.
    fn extension_field(&self) -> Arc<Field> {
        self.chunks.first().unwrap().extension_field()
    }

    fn len(&self) -> usize {
        self.len()
    }

    // fn geometry_chunks(&self) -> Vec<Arc<dyn NativeArray>> {
    //     self.chunks
    //         .iter()
    //         .map(|chunk| Arc::new(chunk.clone()) as NativeArrayRef)
    //         .collect()
    // }

    fn num_chunks(&self) -> usize {
        self.chunks.len()
    }

    // fn as_ref(&self) -> &dyn ChunkedNativeArray {
    //     self
    // }

    fn array_refs(&self) -> Vec<Arc<dyn Array>> {
        self.chunks
            .iter()
            .map(|chunk| chunk.to_array_ref())
            .collect()
    }
}

macro_rules! impl_trait {
    ($chunked_array:ty) => {
        impl<O: OffsetSizeTrait, const D: usize> ChunkedArrayBase for $chunked_array {
            fn as_any(&self) -> &dyn Any {
                self
            }

            // TODO: check/assert on creation that all are the same so we can be comfortable here
            // only taking the first.
            fn extension_field(&self) -> Arc<Field> {
                self.chunks.first().unwrap().extension_field()
            }

            fn len(&self) -> usize {
                self.len()
            }

            fn num_chunks(&self) -> usize {
                self.chunks.len()
            }

            fn array_refs(&self) -> Vec<Arc<dyn Array>> {
                self.chunks
                    .iter()
                    .map(|chunk| chunk.to_array_ref())
                    .collect()
            }
        }

        impl<O: OffsetSizeTrait, const D: usize> ChunkedNativeArray for $chunked_array {
            fn data_type(&self) -> NativeType {
                self.chunks.first().unwrap().data_type()
            }

            fn geometry_chunks(&self) -> Vec<Arc<dyn NativeArray>> {
                self.chunks
                    .iter()
                    .map(|chunk| Arc::new(chunk.clone()) as NativeArrayRef)
                    .collect()
            }

            fn as_ref(&self) -> &dyn ChunkedNativeArray {
                self
            }
        }
    };
}

impl_trait!(ChunkedLineStringArray<O, D>);
impl_trait!(ChunkedPolygonArray<O, D>);
impl_trait!(ChunkedMultiPointArray<O, D>);
impl_trait!(ChunkedMultiLineStringArray<O, D>);
impl_trait!(ChunkedMultiPolygonArray<O, D>);
impl_trait!(ChunkedMixedGeometryArray<O, D>);
impl_trait!(ChunkedGeometryCollectionArray<O, D>);

impl<const D: usize> ChunkedArrayBase for ChunkedRectArray<D> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    // TODO: check/assert on creation that all are the same so we can be comfortable here only
    // taking the first.
    fn extension_field(&self) -> Arc<Field> {
        self.chunks.first().unwrap().extension_field()
    }

    fn len(&self) -> usize {
        self.len()
    }

    fn num_chunks(&self) -> usize {
        self.chunks.len()
    }

    fn array_refs(&self) -> Vec<Arc<dyn Array>> {
        self.chunks
            .iter()
            .map(|chunk| chunk.to_array_ref())
            .collect()
    }
}

impl<const D: usize> ChunkedNativeArray for ChunkedRectArray<D> {
    fn data_type(&self) -> NativeType {
        self.chunks.first().unwrap().data_type()
    }

    fn geometry_chunks(&self) -> Vec<Arc<dyn NativeArray>> {
        self.chunks
            .iter()
            .map(|chunk| Arc::new(chunk.clone()) as NativeArrayRef)
            .collect()
    }

    fn as_ref(&self) -> &dyn ChunkedNativeArray {
        self
    }
}
