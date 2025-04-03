use geoarrow_schema::Metadata;
use crate::array::{RectArray, SeparatedCoordBufferBuilder};
use geoarrow_schema::Dimension;
use crate::error::GeoArrowError;
use crate::scalar::Rect;
use crate::trait_::IntoArrow;
use arrow_array::{ArrayRef, StructArray};
use arrow_buffer::NullBufferBuilder;
use geo_traits::{CoordTrait, RectTrait};
use std::sync::Arc;

/// The GeoArrow equivalent to `Vec<Option<Rect>>`: a mutable collection of Rects.
///
/// Converting an [`RectBuilder`] into a [`RectArray`] is `O(1)`.
#[derive(Debug)]
pub struct RectBuilder {
    pub(crate) metadata: Arc<Metadata>,
    pub(crate) lower: SeparatedCoordBufferBuilder,
    pub(crate) upper: SeparatedCoordBufferBuilder,
    pub(crate) validity: NullBufferBuilder,
}

impl RectBuilder {
    /// Creates a new empty [`RectBuilder`].
    pub fn new(dim: Dimension) -> Self {
        Self::new_with_options(dim, Default::default())
    }

    /// Creates a new empty [`RectBuilder`] with the provided options.
    pub fn new_with_options(dim: Dimension, metadata: Arc<Metadata>) -> Self {
        Self::with_capacity_and_options(dim, 0, metadata)
    }

    /// Creates a new [`RectBuilder`] with a capacity.
    pub fn with_capacity(dim: Dimension, capacity: usize) -> Self {
        Self::with_capacity_and_options(dim, capacity, Default::default())
    }

    /// Creates a new [`RectBuilder`] with a capacity and options.
    pub fn with_capacity_and_options(
        dim: Dimension,
        capacity: usize,
        metadata: Arc<Metadata>,
    ) -> Self {
        Self {
            lower: SeparatedCoordBufferBuilder::with_capacity(capacity, dim),
            upper: SeparatedCoordBufferBuilder::with_capacity(capacity, dim),
            validity: NullBufferBuilder::new(capacity),
            metadata,
        }
    }

    /// Reserves capacity for at least `additional` more Rects.
    ///
    /// The collection may reserve more space to speculatively avoid frequent reallocations. After
    /// calling `reserve`, capacity will be greater than or equal to `self.len() + additional`.
    /// Does nothing if capacity is already sufficient.
    pub fn reserve(&mut self, additional: usize) {
        self.lower.reserve(additional);
        self.upper.reserve(additional);
    }

    /// Reserves the minimum capacity for at least `additional` more Rects.
    ///
    /// Unlike [`reserve`], this will not deliberately over-allocate to speculatively avoid
    /// frequent allocations. After calling `reserve_exact`, capacity will be greater than or equal
    /// to `self.len() + additional`. Does nothing if the capacity is already sufficient.
    ///
    /// Note that the allocator may give the collection more space than it
    /// requests. Therefore, capacity can not be relied upon to be precisely
    /// minimal. Prefer [`reserve`] if future insertions are expected.
    ///
    /// [`reserve`]: Self::reserve
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
        lower: SeparatedCoordBufferBuilder,
        upper: SeparatedCoordBufferBuilder,
        validity: NullBufferBuilder,
        metadata: Arc<Metadata>,
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
        SeparatedCoordBufferBuilder,
        SeparatedCoordBufferBuilder,
        NullBufferBuilder,
    ) {
        (self.lower, self.upper, self.validity)
    }

    /// Convert to an [`ArrayRef`]
    pub fn into_arrow_ref(self) -> ArrayRef {
        Arc::new(self.into_arrow())
    }

    /// Consume the builder and convert to an immutable [`RectArray`]
    pub fn finish(self) -> RectArray {
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
            self.lower.push_nan_coord();
            self.upper.push_nan_coord();
            self.validity.append_null();
        }
    }

    /// Add a new null value to the end of this builder.
    #[inline]
    pub fn push_null(&mut self) {
        self.push_rect(None::<&Rect>);
    }

    /// Push a 2D box to the builder.
    ///
    /// The array should be `[minx, miny, maxx, maxy]`.
    #[inline]
    pub fn push_box2d(&mut self, value: Option<[f64; 4]>) {
        if let Some(value) = value {
            self.lower
                .push_coord(&geo::coord! { x: value[0], y: value[1] });
            self.upper
                .push_coord(&geo::coord! { x: value[2], y: value[3] });
            self.validity.append_non_null()
        } else {
            // Since it's a struct, we still need to push coords when null
            self.lower.push_nan_coord();
            self.upper.push_nan_coord();
            self.validity.append_null();
        }
    }

    /// Push min and max coordinates of a rect to the builder.
    #[inline]
    pub fn push_min_max(&mut self, min: &impl CoordTrait<T = f64>, max: &impl CoordTrait<T = f64>) {
        self.lower.push_coord(min);
        self.upper.push_coord(max);
        self.validity.append_non_null()
    }

    /// Create this builder from a iterator of Rects.
    pub fn from_rects<'a>(
        geoms: impl ExactSizeIterator<Item = &'a (impl RectTrait<T = f64> + 'a)>,
        dim: Dimension,
        metadata: Arc<Metadata>,
    ) -> Self {
        let mut mutable_array = Self::with_capacity_and_options(dim, geoms.len(), metadata);
        geoms
            .into_iter()
            .for_each(|rect| mutable_array.push_rect(Some(rect)));
        mutable_array
    }

    /// Create this builder from a iterator of nullable Rects.
    pub fn from_nullable_rects<'a>(
        geoms: impl ExactSizeIterator<Item = Option<&'a (impl RectTrait<T = f64> + 'a)>>,
        dim: Dimension,
        metadata: Arc<Metadata>,
    ) -> Self {
        let mut mutable_array = Self::with_capacity_and_options(dim, geoms.len(), metadata);
        geoms
            .into_iter()
            .for_each(|maybe_rect| mutable_array.push_rect(maybe_rect));
        mutable_array
    }
}

impl Default for RectBuilder {
    fn default() -> Self {
        Self::new(Dimension::XY)
    }
}

impl IntoArrow for RectBuilder {
    type ArrowArray = StructArray;

    fn into_arrow(self) -> Self::ArrowArray {
        let rect_array: RectArray = self.into();
        rect_array.into_arrow()
    }
}

impl From<RectBuilder> for RectArray {
    fn from(mut other: RectBuilder) -> Self {
        RectArray::new(
            other.lower.into(),
            other.upper.into(),
            other.validity.finish(),
            other.metadata,
        )
    }
}

impl<G: RectTrait<T = f64>> From<(&[G], Dimension)> for RectBuilder {
    fn from((geoms, dim): (&[G], Dimension)) -> Self {
        RectBuilder::from_rects(geoms.iter(), dim, Default::default())
    }
}

impl<G: RectTrait<T = f64>> From<(Vec<Option<G>>, Dimension)> for RectBuilder {
    fn from((geoms, dim): (Vec<Option<G>>, Dimension)) -> Self {
        RectBuilder::from_nullable_rects(geoms.iter().map(|x| x.as_ref()), dim, Default::default())
    }
}
