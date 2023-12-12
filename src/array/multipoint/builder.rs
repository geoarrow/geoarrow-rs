use std::sync::Arc;

// use super::array::check;
use crate::array::offset_builder::OffsetsBuilder;
use crate::array::{
    CoordBufferBuilder, CoordType, InterleavedCoordBufferBuilder, LineStringBuilder,
    MultiPointArray, SeparatedCoordBufferBuilder, WKBArray,
};
use crate::error::{GeoArrowError, Result};
use crate::geo_traits::{MultiPointTrait, PointTrait};
use crate::io::wkb::reader::maybe_multi_point::WKBMaybeMultiPoint;
use crate::scalar::WKB;
use crate::trait_::{GeometryArrayBuilder, IntoArrow};
use arrow_array::{Array, GenericListArray, OffsetSizeTrait};
use arrow_buffer::NullBufferBuilder;

/// The Arrow equivalent to `Vec<Option<MultiPoint>>`.
/// Converting a [`MultiPointBuilder`] into a [`MultiPointArray`] is `O(1)`.
#[derive(Debug)]
pub struct MultiPointBuilder<O: OffsetSizeTrait> {
    coords: CoordBufferBuilder,

    geom_offsets: OffsetsBuilder<O>,

    /// Validity is only defined at the geometry level
    validity: NullBufferBuilder,
}

impl<O: OffsetSizeTrait> MultiPointBuilder<O> {
    /// Creates a new empty [`MultiPointBuilder`].
    pub fn new() -> Self {
        Self::new_with_options(Default::default())
    }

    /// Creates a new [`MultiPointBuilder`] with a specified [`CoordType`]
    pub fn new_with_options(coord_type: CoordType) -> Self {
        Self::with_capacity_and_options(Default::default(), coord_type)
    }
    /// Creates a new [`MultiPointBuilder`] with a capacity.
    pub fn with_capacity(capacity: MultiPointCapacity) -> Self {
        Self::with_capacity_and_options(capacity, Default::default())
    }

    // with capacity and options enables us to write with_capacity based on this method
    pub fn with_capacity_and_options(capacity: MultiPointCapacity, coord_type: CoordType) -> Self {
        let coords = match coord_type {
            CoordType::Interleaved => CoordBufferBuilder::Interleaved(
                InterleavedCoordBufferBuilder::with_capacity(capacity.coord_capacity),
            ),
            CoordType::Separated => CoordBufferBuilder::Separated(
                SeparatedCoordBufferBuilder::with_capacity(capacity.coord_capacity),
            ),
        };
        Self {
            coords,
            geom_offsets: OffsetsBuilder::with_capacity(capacity.geom_capacity),
            validity: NullBufferBuilder::new(capacity.geom_capacity),
        }
    }

    pub fn with_capacity_from_iter<'a>(
        geoms: impl Iterator<Item = Option<&'a (impl MultiPointTrait + 'a)>>,
    ) -> Self {
        Self::with_capacity_and_options_from_iter(geoms, Default::default())
    }

    pub fn with_capacity_and_options_from_iter<'a>(
        geoms: impl Iterator<Item = Option<&'a (impl MultiPointTrait + 'a)>>,
        coord_type: CoordType,
    ) -> Self {
        let counter = MultiPointCapacity::from_multi_points(geoms);
        Self::with_capacity_and_options(counter, coord_type)
    }

    /// Reserves capacity for at least `additional` more MultiPoints to be inserted
    /// in the given `Vec<T>`. The collection may reserve more space to
    /// speculatively avoid frequent reallocations. After calling `reserve`,
    /// capacity will be greater than or equal to `self.len() + additional`.
    /// Does nothing if capacity is already sufficient.
    pub fn reserve(&mut self, capacity: MultiPointCapacity) {
        self.coords.reserve(capacity.coord_capacity);
        self.geom_offsets.reserve(capacity.geom_capacity);
    }

    /// Reserves the minimum capacity for at least `additional` more MultiPoints to
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
    pub fn reserve_exact(&mut self, capacity: MultiPointCapacity) {
        self.coords.reserve_exact(capacity.coord_capacity);
        self.geom_offsets.reserve_exact(capacity.geom_capacity);
    }

    pub fn reserve_from_iter<'a>(
        &mut self,
        geoms: impl Iterator<Item = Option<&'a (impl MultiPointTrait + 'a)>>,
    ) {
        let counter = MultiPointCapacity::from_multi_points(geoms);
        self.reserve(counter)
    }

    pub fn reserve_exact_from_iter<'a>(
        &mut self,
        geoms: impl Iterator<Item = Option<&'a (impl MultiPointTrait + 'a)>>,
    ) {
        let counter = MultiPointCapacity::from_multi_points(geoms);
        self.reserve_exact(counter)
    }

    /// The canonical method to create a [`MultiPointBuilder`] out of its internal components.
    ///
    /// # Implementation
    ///
    /// This function is `O(1)`.
    ///
    /// # Errors
    ///
    /// This function errors iff:
    ///
    /// - if the validity is not `None` and its length is different from the number of geometries
    /// - if the largest geometry offset does not match the number of coordinates
    pub fn try_new(
        coords: CoordBufferBuilder,
        geom_offsets: OffsetsBuilder<O>,
        validity: NullBufferBuilder,
    ) -> Result<Self> {
        // check(
        //     &coords.clone().into(),
        //     validity.as_ref().map(|x| x.len()),
        //     &geom_offsets.clone().into(),
        // )?;
        Ok(Self {
            coords,
            geom_offsets,
            validity,
        })
    }

    /// Extract the low-level APIs from the [`MultiPointBuilder`].
    pub fn into_inner(self) -> (CoordBufferBuilder, OffsetsBuilder<O>, NullBufferBuilder) {
        (self.coords, self.geom_offsets, self.validity)
    }

    pub fn into_array_ref(self) -> Arc<dyn Array> {
        Arc::new(self.into_arrow())
    }

    pub fn extend_from_iter<'a>(
        &mut self,
        geoms: impl Iterator<Item = Option<&'a (impl MultiPointTrait<T = f64> + 'a)>>,
    ) {
        geoms
            .into_iter()
            .try_for_each(|maybe_multi_point| self.push_multi_point(maybe_multi_point))
            .unwrap();
    }

    /// Add a new Point to the end of this array.
    ///
    /// # Errors
    ///
    /// This function errors iff the new last item is larger than what O supports.
    pub fn push_point(&mut self, value: Option<&impl PointTrait<T = f64>>) -> Result<()> {
        if let Some(point) = value {
            self.coords.push_xy(point.x(), point.y());
            self.try_push_length(1)?;
        } else {
            self.push_null();
        }

        Ok(())
    }

    /// Add a new MultiPoint to the end of this array.
    ///
    /// # Errors
    ///
    /// This function errors iff the new last item is larger than what O supports.
    pub fn push_multi_point(
        &mut self,
        value: Option<&impl MultiPointTrait<T = f64>>,
    ) -> Result<()> {
        if let Some(multi_point) = value {
            let num_points = multi_point.num_points();
            for point_idx in 0..num_points {
                let point = multi_point.point(point_idx).unwrap();
                self.coords.push_xy(point.x(), point.y());
            }
            self.try_push_length(num_points)?;
        } else {
            self.push_null();
        }
        Ok(())
    }

    /// Push a raw coordinate to the underlying coordinate array.
    ///
    /// # Safety
    ///
    /// This is marked as unsafe because care must be taken to ensure that pushing raw coordinates
    /// to the array upholds the necessary invariants of the array.
    pub unsafe fn push_xy(&mut self, x: f64, y: f64) -> Result<()> {
        self.coords.push_xy(x, y);
        Ok(())
    }

    fn calculate_added_length(&self) -> Result<usize> {
        let total_length = self.coords.len();
        let offset = self.geom_offsets.last().to_usize().unwrap();
        total_length
            .checked_sub(offset)
            .ok_or(GeoArrowError::Overflow)
    }

    /// Needs to be called when a valid value was extended to this array.
    /// This is a relatively low level function, prefer `try_push` when you can.
    #[inline]
    pub fn try_push_valid(&mut self) -> Result<()> {
        let length = self.calculate_added_length()?;
        self.try_push_length(length)
    }

    /// Needs to be called when a valid value was extended to this array.
    /// This is a relatively low level function, prefer `try_push` when you can.
    #[inline]
    pub fn try_push_length(&mut self, geom_offsets_length: usize) -> Result<()> {
        self.geom_offsets.try_push_usize(geom_offsets_length)?;
        self.validity.append(true);
        Ok(())
    }

    #[inline]
    pub(crate) fn push_null(&mut self) {
        self.geom_offsets.extend_constant(1);
        self.validity.append(false);
    }

    pub fn from_multi_points(
        geoms: &[impl MultiPointTrait<T = f64>],
        coord_type: Option<CoordType>,
    ) -> Self {
        let mut array = Self::with_capacity_and_options_from_iter(
            geoms.iter().map(Some),
            coord_type.unwrap_or_default(),
        );
        array.extend_from_iter(geoms.iter().map(Some));
        array
    }

    pub fn from_nullable_multi_points(
        geoms: &[Option<impl MultiPointTrait<T = f64>>],
        coord_type: Option<CoordType>,
    ) -> Self {
        let mut array = Self::with_capacity_and_options_from_iter(
            geoms.iter().map(|x| x.as_ref()),
            coord_type.unwrap_or_default(),
        );
        array.extend_from_iter(geoms.iter().map(|x| x.as_ref()));
        array
    }

    pub fn from_wkb<W: OffsetSizeTrait>(
        wkb_objects: &[Option<WKB<'_, W>>],
        coord_type: Option<CoordType>,
    ) -> Result<Self> {
        let wkb_objects2: Vec<Option<WKBMaybeMultiPoint>> = wkb_objects
            .iter()
            .map(|maybe_wkb| {
                maybe_wkb
                    .as_ref()
                    .map(|wkb| wkb.to_wkb_object().into_maybe_multi_point())
            })
            .collect();
        Ok(Self::from_nullable_multi_points(&wkb_objects2, coord_type))
    }

    pub fn finish(self) -> MultiPointArray<O> {
        self.into()
    }
}

impl<O: OffsetSizeTrait> Default for MultiPointBuilder<O> {
    fn default() -> Self {
        Self::new()
    }
}

impl<O: OffsetSizeTrait> GeometryArrayBuilder for MultiPointBuilder<O> {
    fn len(&self) -> usize {
        self.coords.len()
    }

    fn validity(&self) -> &NullBufferBuilder {
        &self.validity
    }

    fn into_array_ref(self) -> Arc<dyn Array> {
        self.into_array_ref()
    }
}

impl<O: OffsetSizeTrait> IntoArrow for MultiPointBuilder<O> {
    type ArrowArray = GenericListArray<O>;

    fn into_arrow(self) -> Self::ArrowArray {
        let arr: MultiPointArray<O> = self.into();
        arr.into_arrow()
    }
}

impl<O: OffsetSizeTrait> From<MultiPointBuilder<O>> for MultiPointArray<O> {
    fn from(mut other: MultiPointBuilder<O>) -> Self {
        let validity = other.validity.finish_cloned();

        // TODO: impl shrink_to_fit for all mutable -> * impls
        // other.coords.shrink_to_fit();
        other.geom_offsets.shrink_to_fit();

        Self::new(other.coords.into(), other.geom_offsets.into(), validity)
    }
}

impl<O: OffsetSizeTrait> From<MultiPointBuilder<O>> for GenericListArray<O> {
    fn from(arr: MultiPointBuilder<O>) -> Self {
        arr.into_arrow()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct MultiPointCapacity {
    coord_capacity: usize,
    geom_capacity: usize,
}

impl MultiPointCapacity {
    pub fn new(coord_capacity: usize, geom_capacity: usize) -> Self {
        Self {
            coord_capacity,
            geom_capacity,
        }
    }

    pub fn new_empty() -> Self {
        Self::new(0, 0)
    }

    pub fn is_empty(&self) -> bool {
        self.coord_capacity == 0 && self.geom_capacity == 0
    }

    pub fn add_point<'a>(&mut self, point: Option<&'a (impl PointTrait + 'a)>) {
        self.geom_capacity += 1;
        if let Some(_point) = point {
            self.coord_capacity += 1;
        }
    }

    pub fn add_multi_point<'a>(
        &mut self,
        maybe_multi_point: Option<&'a (impl MultiPointTrait + 'a)>,
    ) {
        self.geom_capacity += 1;

        if let Some(multi_point) = maybe_multi_point {
            self.coord_capacity += multi_point.num_points();
        }
    }

    pub fn add_point_capacity(&mut self, point_capacity: usize) {
        self.coord_capacity += point_capacity;
        self.geom_capacity += point_capacity;
    }

    pub fn coord_capacity(&self) -> usize {
        self.coord_capacity
    }

    pub fn geom_capacity(&self) -> usize {
        self.geom_capacity
    }

    pub fn from_multi_points<'a>(
        geoms: impl Iterator<Item = Option<&'a (impl MultiPointTrait + 'a)>>,
    ) -> Self {
        let mut counter = Self::new_empty();

        for maybe_line_string in geoms.into_iter() {
            counter.add_multi_point(maybe_line_string);
        }

        counter
    }
}

impl Default for MultiPointCapacity {
    fn default() -> Self {
        Self::new_empty()
    }
}

impl<O: OffsetSizeTrait, G: MultiPointTrait<T = f64>> From<&[G]> for MultiPointBuilder<O> {
    fn from(geoms: &[G]) -> Self {
        Self::from_multi_points(geoms, Default::default())
    }
}

impl<O: OffsetSizeTrait, G: MultiPointTrait<T = f64>> From<Vec<Option<G>>>
    for MultiPointBuilder<O>
{
    fn from(geoms: Vec<Option<G>>) -> Self {
        Self::from_nullable_multi_points(&geoms, Default::default())
    }
}

impl<O: OffsetSizeTrait, G: MultiPointTrait<T = f64>> From<bumpalo::collections::Vec<'_, G>>
    for MultiPointBuilder<O>
{
    fn from(geoms: bumpalo::collections::Vec<'_, G>) -> Self {
        Self::from_multi_points(&geoms, Default::default())
    }
}

impl<O: OffsetSizeTrait, G: MultiPointTrait<T = f64>> From<bumpalo::collections::Vec<'_, Option<G>>>
    for MultiPointBuilder<O>
{
    fn from(geoms: bumpalo::collections::Vec<'_, Option<G>>) -> Self {
        Self::from_nullable_multi_points(&geoms, Default::default())
    }
}

impl<O: OffsetSizeTrait> TryFrom<WKBArray<O>> for MultiPointBuilder<O> {
    type Error = GeoArrowError;

    fn try_from(value: WKBArray<O>) -> Result<Self> {
        let wkb_objects: Vec<Option<WKB<'_, O>>> = value.iter().collect();
        Self::from_wkb(&wkb_objects, Default::default())
    }
}

/// LineString and MultiPoint have the same layout, so enable conversions between the two to change
/// the semantic type
impl<O: OffsetSizeTrait> From<MultiPointBuilder<O>> for LineStringBuilder<O> {
    fn from(value: MultiPointBuilder<O>) -> Self {
        Self::try_new(value.coords, value.geom_offsets, value.validity).unwrap()
    }
}
