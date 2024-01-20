use crate::array::metadata::ArrayMetadata;
use crate::array::RectArray;
use crate::error::GeoArrowError;
use crate::geo_traits::{CoordTrait, RectTrait};
use crate::scalar::Rect;
use crate::trait_::IntoArrow;
use arrow_array::{Array, FixedSizeListArray};
use arrow_buffer::NullBufferBuilder;
use std::sync::Arc;

/// The GeoArrow equivalent to `Vec<Option<Rect>>`: a mutable collection of Rects.
///
/// Converting an [`RectBuilder`] into a [`RectArray`] is `O(1)`.
#[derive(Debug)]
pub struct RectBuilder {
    pub metadata: Arc<ArrayMetadata>,
    /// A Buffer of float values for the bounding rectangles
    /// Invariant: the length of values must always be a multiple of 4
    pub values: Vec<f64>,
    pub validity: NullBufferBuilder,
}

impl RectBuilder {
    /// Creates a new empty [`RectBuilder`].
    pub fn new() -> Self {
        Self::with_capacity(0, Default::default())
    }

    /// Creates a new [`RectBuilder`] with a capacity.
    pub fn with_capacity(capacity: usize, metadata: Arc<ArrayMetadata>) -> Self {
        Self {
            values: Vec::with_capacity(capacity * 4),
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
        self.values.reserve(additional * 4);
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
        self.values.reserve_exact(additional * 4);
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
        values: Vec<f64>,
        validity: NullBufferBuilder,
        metadata: Arc<ArrayMetadata>,
    ) -> Result<Self, GeoArrowError> {
        if values.len() != validity.len() * 4 {
            return Err(GeoArrowError::General(
                "Values len should be multiple of 4".to_string(),
            ));
        }
        Ok(Self {
            values,
            validity,
            metadata,
        })
    }

    /// Extract the low-level APIs from the [`RectBuilder`].
    pub fn into_inner(self) -> (Vec<f64>, NullBufferBuilder) {
        (self.values, self.validity)
    }

    /// Add a new Rect to the end of this builder.
    #[inline]
    pub fn push_rect(&mut self, value: Option<&impl RectTrait<T = f64>>) {
        if let Some(value) = value {
            let min_coord = value.lower();
            let max_coord = value.upper();

            self.values.push(min_coord.x());
            self.values.push(min_coord.y());
            self.values.push(max_coord.x());
            self.values.push(max_coord.y());
            self.validity.append_non_null()
        } else {
            // Since it's a fixed size list, we still need to push coords when null
            self.values.push(Default::default());
            self.values.push(Default::default());
            self.values.push(Default::default());
            self.values.push(Default::default());
            self.validity.append_null();
        }
    }

    /// Add a new null value to the end of this builder.
    #[inline]
    pub fn push_null(&mut self) {
        self.push_rect(None::<&Rect>);
    }

    pub fn into_arrow_ref(self) -> Arc<dyn Array> {
        Arc::new(self.into_arrow())
    }

    /// Create this builder from a iterator of Rects.
    pub fn from_rects<'a>(
        geoms: impl ExactSizeIterator + Iterator<Item = &'a (impl RectTrait<T = f64> + 'a)>,
        metadata: Arc<ArrayMetadata>,
    ) -> Self {
        let mut mutable_array = Self::with_capacity(geoms.len(), metadata);
        geoms
            .into_iter()
            .for_each(|rect| mutable_array.push_rect(Some(rect)));
        mutable_array
    }

    /// Create this builder from a iterator of nullable Rects.
    pub fn from_nullable_rects<'a>(
        geoms: impl ExactSizeIterator + Iterator<Item = Option<&'a (impl RectTrait<T = f64> + 'a)>>,
        metadata: Arc<ArrayMetadata>,
    ) -> Self {
        let mut mutable_array = Self::with_capacity(geoms.len(), metadata);
        geoms
            .into_iter()
            .for_each(|maybe_rect| mutable_array.push_rect(maybe_rect));
        mutable_array
    }
}

impl Default for RectBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl IntoArrow for RectBuilder {
    type ArrowArray = FixedSizeListArray;

    fn into_arrow(self) -> Self::ArrowArray {
        let rect_array: RectArray = self.into();
        rect_array.into_arrow()
    }
}

impl From<RectBuilder> for RectArray {
    fn from(other: RectBuilder) -> Self {
        RectArray::new(
            other.values.into(),
            other.validity.finish_cloned(),
            Default::default(),
        )
    }
}

impl<G: RectTrait<T = f64>> From<&[G]> for RectBuilder {
    fn from(geoms: &[G]) -> Self {
        RectBuilder::from_rects(geoms.iter(), Default::default())
    }
}

impl<G: RectTrait<T = f64>> From<Vec<Option<G>>> for RectBuilder {
    fn from(geoms: Vec<Option<G>>) -> Self {
        RectBuilder::from_nullable_rects(geoms.iter().map(|x| x.as_ref()), Default::default())
    }
}
