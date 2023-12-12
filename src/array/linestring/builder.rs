// use super::array::check;
use crate::array::offset_builder::OffsetsBuilder;
use crate::array::{
    CoordBufferBuilder, CoordType, InterleavedCoordBufferBuilder, LineStringArray,
    MultiPointBuilder, SeparatedCoordBufferBuilder, WKBArray,
};
use crate::error::{GeoArrowError, Result};
use crate::geo_traits::LineStringTrait;
use crate::io::wkb::reader::linestring::WKBLineString;
use crate::scalar::WKB;
use crate::trait_::{GeometryArrayBuilder, IntoArrow};
use arrow_array::{Array, GenericListArray, OffsetSizeTrait};
use arrow_buffer::NullBufferBuilder;
use std::convert::From;
use std::sync::Arc;

/// The Arrow equivalent to `Vec<Option<LineString>>`.
/// Converting a [`LineStringBuilder`] into a [`LineStringArray`] is `O(1)`.
#[derive(Debug)]
pub struct LineStringBuilder<O: OffsetSizeTrait> {
    pub(crate) coords: CoordBufferBuilder,

    /// Offsets into the coordinate array where each geometry starts
    pub(crate) geom_offsets: OffsetsBuilder<O>,

    /// Validity is only defined at the geometry level
    pub(crate) validity: NullBufferBuilder,
}

impl<O: OffsetSizeTrait> LineStringBuilder<O> {
    /// Creates a new empty [`LineStringBuilder`].
    pub fn new() -> Self {
        Self::new_with_options(Default::default())
    }

    pub fn new_with_options(coord_type: CoordType) -> Self {
        Self::with_capacity_and_options(Default::default(), coord_type)
    }

    /// Creates a new [`LineStringBuilder`] with a capacity.
    pub fn with_capacity(capacity: LineStringCapacity) -> Self {
        Self::with_capacity_and_options(capacity, Default::default())
    }

    pub fn with_capacity_and_options(capacity: LineStringCapacity, coord_type: CoordType) -> Self {
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
        geoms: impl Iterator<Item = Option<&'a (impl LineStringTrait + 'a)>>,
    ) -> Self {
        Self::with_capacity_and_options_from_iter(geoms, Default::default())
    }

    pub fn with_capacity_and_options_from_iter<'a>(
        geoms: impl Iterator<Item = Option<&'a (impl LineStringTrait + 'a)>>,
        coord_type: CoordType,
    ) -> Self {
        let counter = LineStringCapacity::from_line_strings(geoms);
        Self::with_capacity_and_options(counter, coord_type)
    }

    pub fn reserve_from_iter<'a>(
        &mut self,
        geoms: impl Iterator<Item = Option<&'a (impl LineStringTrait + 'a)>>,
    ) {
        let counter = LineStringCapacity::from_line_strings(geoms);
        self.reserve(counter)
    }

    pub fn reserve_exact_from_iter<'a>(
        &mut self,
        geoms: impl Iterator<Item = Option<&'a (impl LineStringTrait + 'a)>>,
    ) {
        let counter = LineStringCapacity::from_line_strings(geoms);
        self.reserve_exact(counter)
    }

    /// Reserves capacity for at least `additional` more LineStrings to be inserted
    /// in the given `Vec<T>`. The collection may reserve more space to
    /// speculatively avoid frequent reallocations. After calling `reserve`,
    /// capacity will be greater than or equal to `self.len() + additional`.
    /// Does nothing if capacity is already sufficient.
    pub fn reserve(&mut self, additional: LineStringCapacity) {
        self.coords.reserve(additional.coord_capacity);
        self.geom_offsets.reserve(additional.geom_capacity);
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
    pub fn reserve_exact(&mut self, additional: LineStringCapacity) {
        self.coords.reserve_exact(additional.coord_capacity);
        self.geom_offsets.reserve_exact(additional.geom_capacity);
    }

    /// The canonical method to create a [`LineStringBuilder`] out of its internal components.
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

    /// Extract the low-level APIs from the [`LineStringBuilder`].
    pub fn into_inner(self) -> (CoordBufferBuilder, OffsetsBuilder<O>, NullBufferBuilder) {
        (self.coords, self.geom_offsets, self.validity)
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
            let num_coords = line_string.num_coords();
            for coord_idx in 0..num_coords {
                let coord = line_string.coord(coord_idx).unwrap();
                self.coords.push_coord(&coord);
            }
            self.try_push_length(num_coords)?;
        } else {
            self.push_null();
        }
        Ok(())
    }

    pub fn extend_from_iter<'a>(
        &mut self,
        geoms: impl Iterator<Item = Option<&'a (impl LineStringTrait<T = f64> + 'a)>>,
    ) {
        geoms
            .into_iter()
            .try_for_each(|maybe_multi_point| self.push_line_string(maybe_multi_point))
            .unwrap();
    }

    /// Push a raw coordinate to the underlying coordinate array.
    ///
    /// # Safety
    ///
    /// This is marked as unsafe because care must be taken to ensure that pushing raw coordinates
    /// to the array upholds the necessary invariants of the array.
    pub unsafe fn push_xy(&mut self, x: f64, y: f64) {
        self.coords.push_xy(x, y);
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

    pub fn into_array_ref(self) -> Arc<dyn Array> {
        Arc::new(self.into_arrow())
    }

    pub fn from_line_strings(
        geoms: &[impl LineStringTrait<T = f64>],
        coord_type: Option<CoordType>,
    ) -> Self {
        let mut array = Self::with_capacity_and_options_from_iter(
            geoms.iter().map(Some),
            coord_type.unwrap_or_default(),
        );
        array.extend_from_iter(geoms.iter().map(Some));
        array
    }

    pub fn from_nullable_line_strings(
        geoms: &[Option<impl LineStringTrait<T = f64>>],
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
        let wkb_objects2: Vec<Option<WKBLineString>> = wkb_objects
            .iter()
            .map(|maybe_wkb| {
                maybe_wkb
                    .as_ref()
                    .map(|wkb| wkb.to_wkb_object().into_line_string())
            })
            .collect();
        Ok(Self::from_nullable_line_strings(&wkb_objects2, coord_type))
    }

    pub fn finish(self) -> LineStringArray<O> {
        self.into()
    }
}

impl<O: OffsetSizeTrait> GeometryArrayBuilder for LineStringBuilder<O> {
    fn len(&self) -> usize {
        self.geom_offsets.len_proxy()
    }

    fn validity(&self) -> &NullBufferBuilder {
        &self.validity
    }

    fn into_array_ref(self) -> Arc<dyn Array> {
        Arc::new(self.into_arrow())
    }
}

impl<O: OffsetSizeTrait> IntoArrow for LineStringBuilder<O> {
    type ArrowArray = GenericListArray<O>;

    fn into_arrow(self) -> Self::ArrowArray {
        let linestring_arr: LineStringArray<O> = self.into();
        linestring_arr.into_arrow()
    }
}

impl<O: OffsetSizeTrait> Default for LineStringBuilder<O> {
    fn default() -> Self {
        Self::new()
    }
}

impl<O: OffsetSizeTrait> From<LineStringBuilder<O>> for LineStringArray<O> {
    fn from(other: LineStringBuilder<O>) -> Self {
        let validity = other.validity.finish_cloned();
        Self::new(other.coords.into(), other.geom_offsets.into(), validity)
    }
}

impl<O: OffsetSizeTrait> From<LineStringBuilder<O>> for GenericListArray<O> {
    fn from(arr: LineStringBuilder<O>) -> Self {
        arr.into_arrow()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct LineStringCapacity {
    coord_capacity: usize,
    geom_capacity: usize,
}

impl LineStringCapacity {
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

    pub fn add_line_string<'a>(
        &mut self,
        maybe_line_string: Option<&'a (impl LineStringTrait + 'a)>,
    ) {
        self.geom_capacity += 1;
        if let Some(line_string) = maybe_line_string {
            self.coord_capacity += line_string.num_coords();
        }
    }

    pub fn coord_capacity(&self) -> usize {
        self.coord_capacity
    }

    pub fn geom_capacity(&self) -> usize {
        self.geom_capacity
    }

    pub fn from_line_strings<'a>(
        geoms: impl Iterator<Item = Option<&'a (impl LineStringTrait + 'a)>>,
    ) -> Self {
        let mut counter = Self::new_empty();

        for maybe_line_string in geoms.into_iter() {
            counter.add_line_string(maybe_line_string);
        }

        counter
    }
}

impl Default for LineStringCapacity {
    fn default() -> Self {
        Self::new_empty()
    }
}

impl<O: OffsetSizeTrait, G: LineStringTrait<T = f64>> From<&[G]> for LineStringBuilder<O> {
    fn from(geoms: &[G]) -> Self {
        Self::from_line_strings(geoms, Default::default())
    }
}

impl<O: OffsetSizeTrait, G: LineStringTrait<T = f64>> From<Vec<Option<G>>>
    for LineStringBuilder<O>
{
    fn from(geoms: Vec<Option<G>>) -> Self {
        Self::from_nullable_line_strings(&geoms, Default::default())
    }
}

impl<O: OffsetSizeTrait, G: LineStringTrait<T = f64>> From<bumpalo::collections::Vec<'_, G>>
    for LineStringBuilder<O>
{
    fn from(geoms: bumpalo::collections::Vec<'_, G>) -> Self {
        Self::from_line_strings(&geoms, Default::default())
    }
}

impl<O: OffsetSizeTrait, G: LineStringTrait<T = f64>> From<bumpalo::collections::Vec<'_, Option<G>>>
    for LineStringBuilder<O>
{
    fn from(geoms: bumpalo::collections::Vec<'_, Option<G>>) -> Self {
        Self::from_nullable_line_strings(&geoms, Default::default())
    }
}

impl<O: OffsetSizeTrait> TryFrom<WKBArray<O>> for LineStringBuilder<O> {
    type Error = GeoArrowError;

    fn try_from(value: WKBArray<O>) -> Result<Self> {
        let wkb_objects: Vec<Option<WKB<'_, O>>> = value.iter().collect();
        Self::from_wkb(&wkb_objects, Default::default())
    }
}

/// LineString and MultiPoint have the same layout, so enable conversions between the two to change
/// the semantic type
impl<O: OffsetSizeTrait> From<LineStringBuilder<O>> for MultiPointBuilder<O> {
    fn from(value: LineStringBuilder<O>) -> Self {
        Self::try_new(value.coords, value.geom_offsets, value.validity).unwrap()
    }
}
