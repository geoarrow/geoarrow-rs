use crate::array::RectArray;
use crate::error::GeoArrowError;
use crate::geo_traits::{CoordTrait, RectTrait};
use crate::scalar::Rect;
use crate::trait_::IntoArrow;
use arrow_array::{Array, FixedSizeListArray};
use arrow_buffer::NullBufferBuilder;
use std::sync::Arc;

#[derive(Debug)]
pub struct MutableRectArray {
    /// A Buffer of float values for the bounding rectangles
    /// Invariant: the length of values must always be a multiple of 4
    pub values: Vec<f64>,
    pub validity: NullBufferBuilder,
}

impl MutableRectArray {
    /// Creates a new empty [`MutableRectArray`].
    pub fn new() -> Self {
        Self::with_capacity(0)
    }

    /// Creates a new [`MutableRectArray`] with a capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            values: Vec::with_capacity(capacity * 4),
            validity: NullBufferBuilder::new(capacity),
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

    // /// Reserves the minimum capacity for at least `additional` more points to
    // /// be inserted in the given `Vec<T>`. Unlike [`reserve`], this will not
    // /// deliberately over-allocate to speculatively avoid frequent allocations.
    // /// After calling `reserve_exact`, capacity will be greater than or equal to
    // /// `self.len() + additional`. Does nothing if the capacity is already
    // /// sufficient.
    // ///
    // /// Note that the allocator may give the collection more space than it
    // /// requests. Therefore, capacity can not be relied upon to be precisely
    // /// minimal. Prefer [`reserve`] if future insertions are expected.
    // ///
    // /// [`reserve`]: Vec::reserve
    // pub fn reserve_exact(&mut self, additional: usize) {
    //     self.values.reserve_exact(additional * 4);
    // }

    /// The canonical method to create a [`MutableRectArray`] out of its internal components.
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
    pub fn try_new(values: Vec<f64>, validity: NullBufferBuilder) -> Result<Self, GeoArrowError> {
        if values.len() != validity.len() * 4 {
            return Err(GeoArrowError::General(
                "Values len should be multiple of 4".to_string(),
            ));
        }
        Ok(Self { values, validity })
    }

    /// Extract the low-level APIs from the [`MutableRectArray`].
    pub fn into_inner(self) -> (Vec<f64>, NullBufferBuilder) {
        (self.values, self.validity)
    }

    /// Add a new point to the end of this array.
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

    /// Add a new null value to the end of this array.
    #[inline]
    pub fn push_null(&mut self) {
        self.push_rect(None::<&Rect>);
    }

    pub fn into_arrow_ref(self) -> Arc<dyn Array> {
        Arc::new(self.into_arrow())
    }
}

impl Default for MutableRectArray {
    fn default() -> Self {
        Self::new()
    }
}

impl IntoArrow for MutableRectArray {
    type ArrowArray = FixedSizeListArray;

    fn into_arrow(self) -> Self::ArrowArray {
        let rect_array: RectArray = self.into();
        rect_array.into_arrow()
    }
}

impl From<MutableRectArray> for RectArray {
    fn from(other: MutableRectArray) -> Self {
        RectArray::new(other.values.into(), other.validity.finish_cloned())
    }
}

fn first_pass<'a>(
    geoms: impl Iterator<Item = Option<impl RectTrait<T = f64> + 'a>>,
    num_geoms: usize,
) -> MutableRectArray {
    let mut array = MutableRectArray::with_capacity(num_geoms);

    geoms
        .into_iter()
        .for_each(|maybe_rect| array.push_rect(maybe_rect.as_ref()));

    array
}

impl From<Vec<geo::Rect>> for MutableRectArray {
    fn from(geoms: Vec<geo::Rect>) -> Self {
        let num_geoms = geoms.len();
        first_pass(geoms.into_iter().map(Some), num_geoms)
    }
}

impl From<Vec<Option<geo::Rect>>> for MutableRectArray {
    fn from(geoms: Vec<Option<geo::Rect>>) -> Self {
        let num_geoms = geoms.len();
        first_pass(geoms.into_iter(), num_geoms)
    }
}
