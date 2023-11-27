use std::sync::Arc;

// use super::array::check;
use crate::array::mutable_offset::OffsetsBuilder;
use crate::array::{
    CoordType, MultiLineStringArray, MutableCoordBuffer, MutableInterleavedCoordBuffer,
    MutablePolygonArray, MutableSeparatedCoordBuffer, WKBArray,
};
use crate::error::{GeoArrowError, Result};
use crate::geo_traits::{LineStringTrait, MultiLineStringTrait};
use crate::io::wkb::reader::maybe_multi_line_string::WKBMaybeMultiLineString;
use crate::scalar::WKB;
use crate::trait_::IntoArrow;
use arrow_array::{Array, GenericListArray, OffsetSizeTrait};
use arrow_buffer::{NullBufferBuilder, OffsetBuffer};

#[derive(Debug)]
pub struct MutableMultiLineStringArray<O: OffsetSizeTrait> {
    pub(crate) coords: MutableCoordBuffer,

    /// OffsetsBuilder into the ring array where each geometry starts
    pub(crate) geom_offsets: OffsetsBuilder<O>,

    /// OffsetsBuilder into the coordinate array where each ring starts
    pub(crate) ring_offsets: OffsetsBuilder<O>,

    /// Validity is only defined at the geometry level
    pub(crate) validity: NullBufferBuilder,
}

pub type MultiLineStringInner<O> = (
    MutableCoordBuffer,
    OffsetsBuilder<O>,
    OffsetsBuilder<O>,
    NullBufferBuilder,
);

impl<O: OffsetSizeTrait> MutableMultiLineStringArray<O> {
    /// Creates a new empty [`MutableMultiLineStringArray`].
    pub fn new() -> Self {
        MutablePolygonArray::new().into()
    }

    pub fn new_with_options(coord_type: CoordType) -> Self {
        Self::with_capacities_and_options(0, 0, 0, coord_type)
    }

    /// Creates a new [`MutableMultiLineStringArray`] with a capacity.
    pub fn with_capacities(
        coord_capacity: usize,
        ring_capacity: usize,
        geom_capacity: usize,
    ) -> Self {
        Self::with_capacities_and_options(
            coord_capacity,
            ring_capacity,
            geom_capacity,
            Default::default(),
        )
    }

    pub fn with_capacities_and_options(
        coord_capacity: usize,
        ring_capacity: usize,
        geom_capacity: usize,
        coord_type: CoordType,
    ) -> Self {
        let coords = match coord_type {
            CoordType::Interleaved => MutableCoordBuffer::Interleaved(
                MutableInterleavedCoordBuffer::with_capacity(coord_capacity),
            ),
            CoordType::Separated => MutableCoordBuffer::Separated(
                MutableSeparatedCoordBuffer::with_capacity(coord_capacity),
            ),
        };
        Self {
            coords,
            geom_offsets: OffsetsBuilder::with_capacity(geom_capacity),
            ring_offsets: OffsetsBuilder::with_capacity(ring_capacity),
            validity: NullBufferBuilder::new(geom_capacity),
        }
    }

    /// Reserves capacity for at least `additional` more LineStrings to be inserted
    /// in the given `Vec<T>`. The collection may reserve more space to
    /// speculatively avoid frequent reallocations. After calling `reserve`,
    /// capacity will be greater than or equal to `self.len() + additional`.
    /// Does nothing if capacity is already sufficient.
    pub fn reserve(
        &mut self,
        coord_additional: usize,
        ring_additional: usize,
        geom_additional: usize,
    ) {
        self.coords.reserve(coord_additional);
        self.ring_offsets.reserve(ring_additional);
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
    pub fn reserve_exact(
        &mut self,
        coord_additional: usize,
        ring_additional: usize,
        geom_additional: usize,
    ) {
        self.coords.reserve_exact(coord_additional);
        self.ring_offsets.reserve(ring_additional);
        self.geom_offsets.reserve(geom_additional);
    }

    /// The canonical method to create a [`MutableMultiLineStringArray`] out of its internal
    /// components.
    ///
    /// # Implementation
    ///
    /// This function is `O(1)`.
    ///
    /// # Errors
    ///
    /// - if the validity is not `None` and its length is different from the number of geometries
    /// - if the largest ring offset does not match the number of coordinates
    /// - if the largest geometry offset does not match the size of ring offsets
    pub fn try_new(
        coords: MutableCoordBuffer,
        geom_offsets: OffsetsBuilder<O>,
        ring_offsets: OffsetsBuilder<O>,
        validity: NullBufferBuilder,
    ) -> Result<Self> {
        // check(
        //     &coords.clone().into(),
        //     &geom_offsets.clone().into(),
        //     &ring_offsets.clone().into(),
        //     validity.as_ref().map(|x| x.len()),
        // )?;
        Ok(Self {
            coords,
            geom_offsets,
            ring_offsets,
            validity,
        })
    }

    /// Extract the low-level APIs from the [`MutableMultiLineStringArray`].
    pub fn into_inner(self) -> MultiLineStringInner<O> {
        (
            self.coords,
            self.geom_offsets,
            self.ring_offsets,
            self.validity,
        )
    }

    pub fn into_array_ref(self) -> Arc<dyn Array> {
        Arc::new(self.into_arrow())
    }

    /// Add a new LineString to the end of this array.
    ///
    /// # Errors
    ///
    /// This function errors iff the new last item is larger than what O supports.
    pub fn push_line_string(
        &mut self,
        value: Option<&impl LineStringTrait<T = f64>>,
    ) -> Result<()> {
        if let Some(line_string) = value {
            // Total number of linestrings in this multilinestring
            let num_line_strings = 1;
            self.geom_offsets.try_push_usize(num_line_strings)?;

            // For each ring:
            // - Get ring
            // - Add ring's # of coords to self.ring_offsets
            // - Push ring's coords to self.coords

            self.ring_offsets
                .try_push_usize(line_string.num_coords())
                .unwrap();

            for coord_idx in 0..line_string.num_coords() {
                let coord = line_string.coord(coord_idx).unwrap();
                self.coords.push_coord(coord);
            }

            self.validity.append(true);
        } else {
            self.push_null();
        }
        Ok(())
    }

    /// Add a new MultiLineString to the end of this array.
    ///
    /// # Errors
    ///
    /// This function errors iff the new last item is larger than what O supports.
    pub fn push_multi_line_string(
        &mut self,
        value: Option<&impl MultiLineStringTrait<T = f64>>,
    ) -> Result<()> {
        if let Some(multi_line_string) = value {
            // Total number of linestrings in this multilinestring
            let num_line_strings = multi_line_string.num_lines();
            self.geom_offsets.try_push_usize(num_line_strings)?;

            // For each ring:
            // - Get ring
            // - Add ring's # of coords to self.ring_offsets
            // - Push ring's coords to self.coords

            // Number of coords for each ring
            for line_string_idx in 0..num_line_strings {
                let line_string = multi_line_string.line(line_string_idx).unwrap();
                self.ring_offsets
                    .try_push_usize(line_string.num_coords())
                    .unwrap();

                for coord_idx in 0..line_string.num_coords() {
                    let coord = line_string.coord(coord_idx).unwrap();
                    self.coords.push_coord(coord);
                }
            }

            self.validity.append(true);
        } else {
            self.push_null();
        }
        Ok(())
    }

    /// Push a raw offset to the underlying geometry offsets buffer.
    ///
    /// # Safety
    ///
    /// This is marked as unsafe because care must be taken to ensure that pushing raw offsets
    /// upholds the necessary invariants of the array.
    pub unsafe fn try_push_geom_offset(&mut self, offsets_length: usize) -> Result<()> {
        self.geom_offsets.try_push_usize(offsets_length)?;
        self.validity.append(true);
        Ok(())
    }

    /// Push a raw offset to the underlying ring offsets buffer.
    ///
    /// # Safety
    ///
    /// This is marked as unsafe because care must be taken to ensure that pushing raw offsets
    /// upholds the necessary invariants of the array.
    pub unsafe fn try_push_ring_offset(&mut self, offsets_length: usize) -> Result<()> {
        self.ring_offsets.try_push_usize(offsets_length)?;
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

    #[inline]
    pub(crate) fn push_null(&mut self) {
        // NOTE! Only the geom_offsets array needs to get extended, because the next geometry will
        // point to the same ring array location
        self.geom_offsets.extend_constant(1);
        self.validity.append(false);
    }
}

impl<O: OffsetSizeTrait> IntoArrow for MutableMultiLineStringArray<O> {
    type ArrowArray = GenericListArray<O>;

    fn into_arrow(self) -> Self::ArrowArray {
        let arr: MultiLineStringArray<O> = self.into();
        arr.into_arrow()
    }
}

impl<O: OffsetSizeTrait> Default for MutableMultiLineStringArray<O> {
    fn default() -> Self {
        Self::new()
    }
}

impl<O: OffsetSizeTrait> From<MutableMultiLineStringArray<O>> for MultiLineStringArray<O> {
    fn from(other: MutableMultiLineStringArray<O>) -> Self {
        let validity = other.validity.finish_cloned();

        let geom_offsets: OffsetBuffer<O> = other.geom_offsets.into();
        let ring_offsets: OffsetBuffer<O> = other.ring_offsets.into();

        Self::new(other.coords.into(), geom_offsets, ring_offsets, validity)
    }
}

fn first_pass<'a>(
    geoms: impl Iterator<Item = Option<&'a (impl MultiLineStringTrait + 'a)>>,
    geoms_length: usize,
) -> (usize, usize, usize) {
    // Total number of coordinates
    let mut coord_capacity = 0;
    let mut ring_capacity = 0;
    let geom_capacity = geoms_length;

    for multi_line_string in geoms.into_iter().flatten() {
        // Total number of rings in this polygon
        let num_line_strings = multi_line_string.num_lines();
        ring_capacity += num_line_strings;

        for line_string_idx in 0..num_line_strings {
            let line_string = multi_line_string.line(line_string_idx).unwrap();
            coord_capacity += line_string.num_coords();
        }
    }

    // TODO: dataclass for capacities to access them by name?
    (coord_capacity, ring_capacity, geom_capacity)
}

fn second_pass<'a, O: OffsetSizeTrait>(
    geoms: impl Iterator<Item = Option<&'a (impl MultiLineStringTrait<T = f64> + 'a)>>,
    coord_capacity: usize,
    ring_capacity: usize,
    geom_capacity: usize,
) -> MutableMultiLineStringArray<O> {
    let mut array =
        MutableMultiLineStringArray::with_capacities(coord_capacity, ring_capacity, geom_capacity);

    geoms
        .into_iter()
        .try_for_each(|maybe_multi_line_string| {
            array.push_multi_line_string(maybe_multi_line_string)
        })
        .unwrap();

    array
}

impl<O: OffsetSizeTrait, G: MultiLineStringTrait<T = f64>> From<Vec<G>>
    for MutableMultiLineStringArray<O>
{
    fn from(geoms: Vec<G>) -> Self {
        let (coord_capacity, ring_capacity, geom_capacity) =
            first_pass(geoms.iter().map(Some), geoms.len());
        second_pass(
            geoms.iter().map(Some),
            coord_capacity,
            ring_capacity,
            geom_capacity,
        )
    }
}

impl<O: OffsetSizeTrait, G: MultiLineStringTrait<T = f64>> From<Vec<Option<G>>>
    for MutableMultiLineStringArray<O>
{
    fn from(geoms: Vec<Option<G>>) -> Self {
        let (coord_capacity, ring_capacity, geom_capacity) =
            first_pass(geoms.iter().map(|x| x.as_ref()), geoms.len());
        second_pass(
            geoms.iter().map(|x| x.as_ref()),
            coord_capacity,
            ring_capacity,
            geom_capacity,
        )
    }
}

impl<O: OffsetSizeTrait, G: MultiLineStringTrait<T = f64>> From<bumpalo::collections::Vec<'_, G>>
    for MutableMultiLineStringArray<O>
{
    fn from(geoms: bumpalo::collections::Vec<'_, G>) -> Self {
        let (coord_capacity, ring_capacity, geom_capacity) =
            first_pass(geoms.iter().map(Some), geoms.len());
        second_pass(
            geoms.iter().map(Some),
            coord_capacity,
            ring_capacity,
            geom_capacity,
        )
    }
}

impl<O: OffsetSizeTrait, G: MultiLineStringTrait<T = f64>>
    From<bumpalo::collections::Vec<'_, Option<G>>> for MutableMultiLineStringArray<O>
{
    fn from(geoms: bumpalo::collections::Vec<'_, Option<G>>) -> Self {
        let (coord_capacity, ring_capacity, geom_capacity) =
            first_pass(geoms.iter().map(|x| x.as_ref()), geoms.len());
        second_pass(
            geoms.iter().map(|x| x.as_ref()),
            coord_capacity,
            ring_capacity,
            geom_capacity,
        )
    }
}

impl<O: OffsetSizeTrait> TryFrom<WKBArray<O>> for MutableMultiLineStringArray<O> {
    type Error = GeoArrowError;

    fn try_from(value: WKBArray<O>) -> Result<Self> {
        let wkb_objects: Vec<Option<WKB<'_, O>>> = value.iter().collect();
        let wkb_objects2: Vec<Option<WKBMaybeMultiLineString>> = wkb_objects
            .iter()
            .map(|maybe_wkb| {
                maybe_wkb
                    .as_ref()
                    .map(|wkb| wkb.to_wkb_object().into_maybe_multi_line_string())
            })
            .collect();
        Ok(wkb_objects2.into())
    }
}

/// Polygon and MultiLineString have the same layout, so enable conversions between the two to
/// change the semantic type
impl<O: OffsetSizeTrait> From<MutableMultiLineStringArray<O>> for MutablePolygonArray<O> {
    fn from(value: MutableMultiLineStringArray<O>) -> Self {
        Self::try_new(
            value.coords,
            value.geom_offsets,
            value.ring_offsets,
            value.validity,
        )
        .unwrap()
    }
}
