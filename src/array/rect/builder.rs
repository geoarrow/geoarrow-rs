use crate::array::metadata::ArrayMetadata;
use crate::array::{RectArray, SeparatedCoordBufferBuilder};
use crate::error::GeoArrowError;
use crate::geo_traits::RectTrait;
use crate::scalar::Rect;
use crate::trait_::IntoArrow;
use arrow_array::{Array, StructArray};
use arrow_buffer::NullBufferBuilder;
use std::sync::Arc;

/// The GeoArrow equivalent to `Vec<Option<Rect>>`: a mutable collection of Rects.
///
/// Converting an [`RectBuilder`] into a [`RectArray`] is `O(1)`.
#[derive(Debug)]
pub struct RectBuilder<const D: usize> {
    pub metadata: Arc<ArrayMetadata>,
    pub lower: SeparatedCoordBufferBuilder<D>,
    pub upper: SeparatedCoordBufferBuilder<D>,
    pub validity: NullBufferBuilder,
}

impl<const D: usize> RectBuilder<D> {
    /// Creates a new empty [`RectBuilder`].
    pub fn new() -> Self {
        Self::new_with_options(Default::default())
    }

    pub fn new_with_options(metadata: Arc<ArrayMetadata>) -> Self {
        Self::with_capacity_and_options(0, metadata)
    }

    /// Creates a new [`RectBuilder`] with a capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self::with_capacity_and_options(capacity, Default::default())
    }

    /// Creates a new [`RectBuilder`] with a capacity.
    pub fn with_capacity_and_options(capacity: usize, metadata: Arc<ArrayMetadata>) -> Self {
        Self {
            lower: SeparatedCoordBufferBuilder::with_capacity(capacity),
            upper: SeparatedCoordBufferBuilder::with_capacity(capacity),
            validity: NullBufferBuilder::new(capacity),
            metadata,
        }
    }

    /// Reserves capacity for at least `additional` more points to be inserted
    /// in the given `Vec<T>`. The collection may reserve more space to
    /// speculatively avoid frequent reallocations. After calling `reserve`,
    /// capacity will be greater than or equal to `self.len() + additional`.
    /// Does nothing if capacity is already sufficient.
    pub fn reserve(&mut self, additional: usize) {
        self.lower.reserve(additional);
        self.upper.reserve(additional);
    }

    /// Reserves the minimum capacity for at least `additional` more points to
    /// be inserted in the given `Vec<T>`. Unlike [`reserve`], this will not
    /// deliberately over-allocate to speculatively avoid frequent allocations.
    /// After calling `reserve_exact`, capacity will be greater than or equal to
    /// `self.len() + additional`. Does nothing if the capacity is already
    /// sufficient.
    ///
    /// Note that the allocator may give the collection more space than it
    /// requests. Therefore, capacity can not be relied upon to be precisely
    /// minimal. Prefer [`reserve`] if future insertions are expected.
    ///
    /// [`reserve`]: Vec::reserve
    pub fn reserve_exact(&mut self, additional: usize) {
        self.lower.reserve_exact(additional);
        self.upper.reserve_exact(additional);
    }

    /// The canonical method to create a [`RectBuilder`] out of its internal components.
    ///
    /// # Implementation
    ///
    /// This function is `O(1)`.
    ///
    /// # Errors
    ///
    /// This function errors iff:
    ///
    /// - The validity is not `None` and its length is different from the number of geometries
    pub fn try_new(
        lower: SeparatedCoordBufferBuilder<D>,
        upper: SeparatedCoordBufferBuilder<D>,
        validity: NullBufferBuilder,
        metadata: Arc<ArrayMetadata>,
    ) -> Result<Self, GeoArrowError> {
        if lower.len() != upper.len() {
            return Err(GeoArrowError::General(
                "Lower and upper lengths must match".to_string(),
            ));
        }
        Ok(Self {
            lower,
            upper,
            validity,
            metadata,
        })
    }

    /// Extract the low-level APIs from the [`RectBuilder`].
    pub fn into_inner(
        self,
    ) -> (
        SeparatedCoordBufferBuilder<D>,
        SeparatedCoordBufferBuilder<D>,
        NullBufferBuilder,
    ) {
        (self.lower, self.upper, self.validity)
    }

    pub fn into_arrow_ref(self) -> Arc<dyn Array> {
        Arc::new(self.into_arrow())
    }

    pub fn finish(self) -> RectArray<D> {
        self.into()
    }

    /// Add a new Rect to the end of this builder.
    #[inline]
    pub fn push_rect(&mut self, value: Option<&impl RectTrait<T = f64>>) {
        if let Some(value) = value {
            let min_coord = value.min();
            let max_coord = value.max();

            self.lower.push_coord(&min_coord);
            self.upper.push_coord(&max_coord);
            self.validity.append_non_null()
        } else {
            // Since it's a struct, we still need to push coords when null
            self.lower.push(core::array::from_fn(|_| 0.));
            self.upper.push(core::array::from_fn(|_| 0.));
            self.validity.append_null();
        }
    }

    /// Add a new null value to the end of this builder.
    #[inline]
    pub fn push_null(&mut self) {
        self.push_rect(None::<&Rect<D>>);
    }

    /// Create this builder from a iterator of Rects.
    pub fn from_rects<'a>(
        geoms: impl ExactSizeIterator<Item = &'a (impl RectTrait<T = f64> + 'a)>,
        metadata: Arc<ArrayMetadata>,
    ) -> Self {
        let mut mutable_array = Self::with_capacity_and_options(geoms.len(), metadata);
        geoms
            .into_iter()
            .for_each(|rect| mutable_array.push_rect(Some(rect)));
        mutable_array
    }

    /// Create this builder from a iterator of nullable Rects.
    pub fn from_nullable_rects<'a>(
        geoms: impl ExactSizeIterator<Item = Option<&'a (impl RectTrait<T = f64> + 'a)>>,
        metadata: Arc<ArrayMetadata>,
    ) -> Self {
        let mut mutable_array = Self::with_capacity_and_options(geoms.len(), metadata);
        geoms
            .into_iter()
            .for_each(|maybe_rect| mutable_array.push_rect(maybe_rect));
        mutable_array
    }
}

impl<const D: usize> Default for RectBuilder<D> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const D: usize> IntoArrow for RectBuilder<D> {
    type ArrowArray = StructArray;

    fn into_arrow(self) -> Self::ArrowArray {
        let rect_array: RectArray<D> = self.into();
        rect_array.into_arrow()
    }
}

impl<const D: usize> From<RectBuilder<D>> for RectArray<D> {
    fn from(mut other: RectBuilder<D>) -> Self {
        RectArray::new(
            other.lower.into(),
            other.upper.into(),
            other.validity.finish(),
            Default::default(),
        )
    }
}

impl<G: RectTrait<T = f64>, const D: usize> From<&[G]> for RectBuilder<D> {
    fn from(geoms: &[G]) -> Self {
        RectBuilder::from_rects(geoms.iter(), Default::default())
    }
}

impl<G: RectTrait<T = f64>, const D: usize> From<Vec<Option<G>>> for RectBuilder<D> {
    fn from(geoms: Vec<Option<G>>) -> Self {
        RectBuilder::from_nullable_rects(geoms.iter().map(|x| x.as_ref()), Default::default())
    }
}
