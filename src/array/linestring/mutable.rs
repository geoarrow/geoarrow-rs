use super::array::check;
use crate::array::mutable_offset::Offsets;
use crate::array::{
    LineStringArray, MutableCoordBuffer, MutableInterleavedCoordBuffer, MutableMultiPointArray,
    WKBArray,
};
use crate::error::{GeoArrowError, Result};
use crate::geo_traits::{CoordTrait, LineStringTrait};
use crate::io::wkb::reader::linestring::WKBLineString;
use crate::scalar::WKB;
use crate::GeometryArrayTrait;
use arrow_array::{Array, GenericListArray, OffsetSizeTrait};
use arrow_buffer::{BufferBuilder, NullBuffer, NullBufferBuilder};
use std::convert::From;
use std::sync::Arc;

/// The Arrow equivalent to `Vec<Option<LineString>>`.
/// Converting a [`MutableLineStringArray`] into a [`LineStringArray`] is `O(1)`.
#[derive(Debug)]
pub struct MutableLineStringArray<O: OffsetSizeTrait> {
    coords: MutableCoordBuffer,

    /// Offsets into the coordinate array where each geometry starts
    geom_offsets: Offsets<O>,

    /// Validity is only defined at the geometry level
    validity: NullBufferBuilder,
}

impl<'a, O: OffsetSizeTrait> MutableLineStringArray<O> {
    /// Creates a new empty [`MutableLineStringArray`].
    pub fn new() -> Self {
        Self::with_capacities(0, 0)
    }

    /// Creates a new [`MutableLineStringArray`] with a capacity.
    pub fn with_capacities(coord_capacity: usize, geom_capacity: usize) -> Self {
        let coords = MutableInterleavedCoordBuffer::with_capacity(coord_capacity);
        Self {
            coords: MutableCoordBuffer::Interleaved(coords),
            geom_offsets: Offsets::with_capacity(geom_capacity),
            validity: NullBufferBuilder::new(geom_capacity),
        }
    }

    /// Reserves capacity for at least `additional` more LineStrings to be inserted
    /// in the given `Vec<T>`. The collection may reserve more space to
    /// speculatively avoid frequent reallocations. After calling `reserve`,
    /// capacity will be greater than or equal to `self.len() + additional`.
    /// Does nothing if capacity is already sufficient.
    pub fn reserve(&mut self, coord_additional: usize, geom_additional: usize) {
        self.coords.reserve(coord_additional);
        self.geom_offsets.reserve(geom_additional);
    }

    /// Reserves the minimum capacity for at least `additional` more LineStrings to
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

    /// The canonical method to create a [`MutableLineStringArray`] out of its internal components.
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
    /// - if the largest geometry offset does not match the number of coordinates
    pub fn try_new(
        coords: MutableCoordBuffer,
        geom_offsets: Offsets<O>,
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

    /// Extract the low-level APIs from the [`MutableLineStringArray`].
    pub fn into_inner(self) -> (MutableCoordBuffer, Offsets<O>, NullBufferBuilder) {
        (self.coords, self.geom_offsets, self.validity)
    }

    /// Add a new LineString to the end of this array.
    ///
    /// # Errors
    ///
    /// This function errors iff the new last item is larger than what O supports.
    pub fn push_line_string(
        &mut self,
        value: Option<&impl LineStringTrait<'a, T = f64>>,
    ) -> Result<()> {
        if let Some(line_string) = value {
            let num_coords = line_string.num_coords();
            for coord_idx in 0..num_coords {
                let coord = line_string.coord(coord_idx).unwrap();
                self.coords.push_xy(coord.x(), coord.y());
            }
            self.try_push_length(num_coords)?;
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

    /// Needs to be called when a valid value was extended to this array.
    /// This is a relatively low level function, prefer `try_push` when you can.
    #[inline]
    pub fn try_push_length(&mut self, geom_offsets_length: usize) -> Result<()> {
        self.geom_offsets.try_push_usize(geom_offsets_length)?;
        self.validity.append(true);
        Ok(())
    }

    #[inline]
    fn push_null(&mut self) {
        self.geom_offsets.extend_constant(1);
        self.validity.append(false);
    }

    pub fn into_arrow(self) -> GenericListArray<O> {
        let linestring_arr: LineStringArray<O> = self.into();
        linestring_arr.into_arrow()
    }

    pub fn into_array_ref(self) -> Arc<dyn Array> {
        Arc::new(self.into_arrow())
    }
}

impl<O: OffsetSizeTrait> Default for MutableLineStringArray<O> {
    fn default() -> Self {
        Self::new()
    }
}

impl<O: OffsetSizeTrait> From<MutableLineStringArray<O>> for LineStringArray<O> {
    fn from(other: MutableLineStringArray<O>) -> Self {
        let validity = other.validity.finish_cloned();
        Self::new(other.coords.into(), other.geom_offsets.into(), validity)
    }
}

impl<O: OffsetSizeTrait> From<MutableLineStringArray<O>> for GenericListArray<O> {
    fn from(arr: MutableLineStringArray<O>) -> Self {
        arr.into_arrow()
    }
}

pub(crate) fn first_pass<'a>(
    geoms: impl Iterator<Item = Option<impl LineStringTrait<'a>>>,
    geoms_length: usize,
) -> (usize, usize) {
    let mut coord_capacity = 0;
    let geom_capacity = geoms_length;

    for line_string in geoms.into_iter().flatten() {
        coord_capacity += line_string.num_coords();
    }

    (coord_capacity, geom_capacity)
}

pub(crate) fn second_pass<'a, O: OffsetSizeTrait>(
    geoms: impl Iterator<Item = Option<impl LineStringTrait<'a, T = f64>>>,
    coord_capacity: usize,
    geom_capacity: usize,
) -> MutableLineStringArray<O> {
    let mut array = MutableLineStringArray::with_capacities(coord_capacity, geom_capacity);

    geoms
        .into_iter()
        .try_for_each(|maybe_multi_point| array.push_line_string(maybe_multi_point.as_ref()))
        .unwrap();

    array
}

impl<O: OffsetSizeTrait> From<Vec<geo::LineString>> for MutableLineStringArray<O> {
    fn from(geoms: Vec<geo::LineString>) -> Self {
        let (coord_capacity, geom_capacity) = first_pass(geoms.iter().map(Some), geoms.len());
        second_pass(geoms.into_iter().map(Some), coord_capacity, geom_capacity)
    }
}

impl<O: OffsetSizeTrait> From<Vec<Option<geo::LineString>>> for MutableLineStringArray<O> {
    fn from(geoms: Vec<Option<geo::LineString>>) -> Self {
        let (coord_capacity, geom_capacity) =
            first_pass(geoms.iter().map(|x| x.as_ref()), geoms.len());
        second_pass(geoms.into_iter(), coord_capacity, geom_capacity)
    }
}

impl<O: OffsetSizeTrait> From<bumpalo::collections::Vec<'_, geo::LineString>>
    for MutableLineStringArray<O>
{
    fn from(geoms: bumpalo::collections::Vec<'_, geo::LineString>) -> Self {
        let (coord_capacity, geom_capacity) = first_pass(geoms.iter().map(Some), geoms.len());
        second_pass(geoms.into_iter().map(Some), coord_capacity, geom_capacity)
    }
}

impl<O: OffsetSizeTrait> From<bumpalo::collections::Vec<'_, Option<geo::LineString>>>
    for MutableLineStringArray<O>
{
    fn from(geoms: bumpalo::collections::Vec<'_, Option<geo::LineString>>) -> Self {
        let (coord_capacity, geom_capacity) =
            first_pass(geoms.iter().map(|x| x.as_ref()), geoms.len());
        second_pass(geoms.into_iter(), coord_capacity, geom_capacity)
    }
}

impl<O: OffsetSizeTrait> TryFrom<WKBArray<O>> for MutableLineStringArray<O> {
    type Error = GeoArrowError;

    fn try_from(value: WKBArray<O>) -> Result<Self> {
        let wkb_objects: Vec<Option<WKB<'_, O>>> = value.iter().collect();
        let wkb_objects2: Vec<Option<WKBLineString>> = wkb_objects
            .iter()
            .map(|maybe_wkb| {
                maybe_wkb
                    .as_ref()
                    .map(|wkb| wkb.to_wkb_object().into_line_string())
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
impl<O: OffsetSizeTrait> From<MutableLineStringArray<O>> for MutableMultiPointArray<O> {
    fn from(value: MutableLineStringArray<O>) -> Self {
        Self::try_new(value.coords, value.geom_offsets, value.validity).unwrap()
    }
}
