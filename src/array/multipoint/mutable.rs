use std::sync::Arc;

// use super::array::check;
use crate::array::mutable_offset::OffsetsBuilder;
use crate::array::{
    MultiPointArray, MutableCoordBuffer, MutableInterleavedCoordBuffer, MutableLineStringArray,
    WKBArray,
};
use crate::error::{GeoArrowError, Result};
use crate::geo_traits::{MultiPointTrait, PointTrait};
use crate::io::wkb::reader::maybe_multi_point::WKBMaybeMultiPoint;
use crate::scalar::WKB;
use crate::trait_::{GeometryArrayTrait, MutableGeometryArray};
use arrow_array::{Array, GenericListArray, OffsetSizeTrait};
use arrow_buffer::NullBufferBuilder;

/// The Arrow equivalent to `Vec<Option<MultiPoint>>`.
/// Converting a [`MutableMultiPointArray`] into a [`MultiPointArray`] is `O(1)`.
#[derive(Debug)]
pub struct MutableMultiPointArray<O: OffsetSizeTrait> {
    coords: MutableCoordBuffer,

    geom_offsets: OffsetsBuilder<O>,

    /// Validity is only defined at the geometry level
    validity: NullBufferBuilder,
}

impl<'a, O: OffsetSizeTrait> MutableMultiPointArray<O> {
    /// Creates a new empty [`MutableMultiPointArray`].
    pub fn new() -> Self {
        Self::with_capacities(0, 0)
    }

    /// Creates a new [`MutableMultiPointArray`] with a capacity.
    pub fn with_capacities(coord_capacity: usize, geom_capacity: usize) -> Self {
        let coords = MutableInterleavedCoordBuffer::with_capacity(coord_capacity);
        Self {
            coords: MutableCoordBuffer::Interleaved(coords),
            geom_offsets: OffsetsBuilder::with_capacity(geom_capacity),
            validity: NullBufferBuilder::new(geom_capacity),
        }
    }

    /// Reserves capacity for at least `additional` more MultiPoints to be inserted
    /// in the given `Vec<T>`. The collection may reserve more space to
    /// speculatively avoid frequent reallocations. After calling `reserve`,
    /// capacity will be greater than or equal to `self.len() + additional`.
    /// Does nothing if capacity is already sufficient.
    pub fn reserve(&mut self, coord_additional: usize, geom_additional: usize) {
        self.coords.reserve(coord_additional);
        self.geom_offsets.reserve(geom_additional);
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
    pub fn reserve_exact(&mut self, coord_additional: usize, geom_additional: usize) {
        self.coords.reserve_exact(coord_additional);
        self.geom_offsets.reserve(geom_additional);
    }

    /// The canonical method to create a [`MutableMultiPointArray`] out of its internal components.
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
        coords: MutableCoordBuffer,
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

    /// Extract the low-level APIs from the [`MutableMultiPointArray`].
    pub fn into_inner(self) -> (MutableCoordBuffer, OffsetsBuilder<O>, NullBufferBuilder) {
        (self.coords, self.geom_offsets, self.validity)
    }

    pub fn into_arrow(self) -> GenericListArray<O> {
        let arr: MultiPointArray<O> = self.into();
        arr.into_arrow()
    }

    pub fn into_array_ref(self) -> Arc<dyn Array> {
        Arc::new(self.into_arrow())
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
        value: Option<&impl MultiPointTrait<'a, T = f64>>,
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
}

impl<O: OffsetSizeTrait> Default for MutableMultiPointArray<O> {
    fn default() -> Self {
        Self::new()
    }
}

impl<O: OffsetSizeTrait> MutableGeometryArray for MutableMultiPointArray<O> {
    fn len(&self) -> usize {
        self.coords.len()
    }

    fn validity(&self) -> &NullBufferBuilder {
        &self.validity
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn into_array_ref(self) -> Arc<dyn Array> {
        Arc::new(self.into_arrow())
    }
}

impl<O: OffsetSizeTrait> From<MutableMultiPointArray<O>> for MultiPointArray<O> {
    fn from(mut other: MutableMultiPointArray<O>) -> Self {
        let validity = other.validity.finish_cloned();

        // TODO: impl shrink_to_fit for all mutable -> * impls
        // other.coords.shrink_to_fit();
        other.geom_offsets.shrink_to_fit();

        Self::new(other.coords.into(), other.geom_offsets.into(), validity)
    }
}

impl<O: OffsetSizeTrait> From<MutableMultiPointArray<O>> for GenericListArray<O> {
    fn from(arr: MutableMultiPointArray<O>) -> Self {
        arr.into_arrow()
    }
}

fn first_pass<'a>(
    geoms: impl Iterator<Item = Option<impl MultiPointTrait<'a> + 'a>>,
    geoms_length: usize,
) -> (usize, usize) {
    let mut coord_capacity = 0;
    let geom_capacity = geoms_length;

    for multi_point in geoms.into_iter().flatten() {
        coord_capacity += multi_point.num_points();
    }

    (coord_capacity, geom_capacity)
}

fn second_pass<'a, O: OffsetSizeTrait>(
    geoms: impl Iterator<Item = Option<impl MultiPointTrait<'a, T = f64> + 'a>>,
    coord_capacity: usize,
    geom_capacity: usize,
) -> MutableMultiPointArray<O> {
    let mut array = MutableMultiPointArray::with_capacities(coord_capacity, geom_capacity);

    geoms
        .into_iter()
        .try_for_each(|maybe_multi_point| array.push_multi_point(maybe_multi_point.as_ref()))
        .unwrap();

    array
}

impl<O: OffsetSizeTrait> From<Vec<geo::MultiPoint>> for MutableMultiPointArray<O> {
    fn from(geoms: Vec<geo::MultiPoint>) -> Self {
        let (coord_capacity, geom_capacity) = first_pass(geoms.iter().map(Some), geoms.len());
        second_pass(geoms.into_iter().map(Some), coord_capacity, geom_capacity)
    }
}

impl<O: OffsetSizeTrait> From<Vec<Option<geo::MultiPoint>>> for MutableMultiPointArray<O> {
    fn from(geoms: Vec<Option<geo::MultiPoint>>) -> Self {
        let (coord_capacity, geom_capacity) =
            first_pass(geoms.iter().map(|x| x.as_ref()), geoms.len());
        second_pass(geoms.into_iter(), coord_capacity, geom_capacity)
    }
}

impl<O: OffsetSizeTrait> From<bumpalo::collections::Vec<'_, geo::MultiPoint>>
    for MutableMultiPointArray<O>
{
    fn from(geoms: bumpalo::collections::Vec<'_, geo::MultiPoint>) -> Self {
        let (coord_capacity, geom_capacity) = first_pass(geoms.iter().map(Some), geoms.len());
        second_pass(geoms.into_iter().map(Some), coord_capacity, geom_capacity)
    }
}

impl<O: OffsetSizeTrait> From<bumpalo::collections::Vec<'_, Option<geo::MultiPoint>>>
    for MutableMultiPointArray<O>
{
    fn from(geoms: bumpalo::collections::Vec<'_, Option<geo::MultiPoint>>) -> Self {
        let (coord_capacity, geom_capacity) =
            first_pass(geoms.iter().map(|x| x.as_ref()), geoms.len());
        second_pass(geoms.into_iter(), coord_capacity, geom_capacity)
    }
}

impl<O: OffsetSizeTrait> TryFrom<WKBArray<O>> for MutableMultiPointArray<O> {
    type Error = GeoArrowError;

    fn try_from(value: WKBArray<O>) -> Result<Self> {
        let wkb_objects: Vec<Option<WKB<'_, O>>> = value.iter().collect();
        let wkb_objects2: Vec<Option<WKBMaybeMultiPoint>> = wkb_objects
            .iter()
            .map(|maybe_wkb| {
                maybe_wkb
                    .as_ref()
                    .map(|wkb| wkb.to_wkb_object().into_maybe_multi_point())
            })
            .collect();
        let (coord_capacity, geom_capacity) =
            first_pass(wkb_objects2.iter().map(|item| item.as_ref()), value.len());
        Ok(second_pass(
            wkb_objects2.iter().map(|item| item.as_ref()),
            coord_capacity,
            geom_capacity,
        ))
    }
}

/// LineString and MultiPoint have the same layout, so enable conversions between the two to change
/// the semantic type
impl<O: OffsetSizeTrait> From<MutableMultiPointArray<O>> for MutableLineStringArray<O> {
    fn from(value: MutableMultiPointArray<O>) -> Self {
        Self::try_new(value.coords, value.geom_offsets, value.validity).unwrap()
    }
}
