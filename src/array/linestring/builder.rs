use crate::array::linestring::capacity::LineStringCapacity;
use crate::array::metadata::ArrayMetadata;
// use super::array::check;
use crate::array::offset_builder::OffsetsBuilder;
use crate::array::{
    CoordBufferBuilder, CoordType, InterleavedCoordBufferBuilder, LineStringArray,
    MultiPointBuilder, SeparatedCoordBufferBuilder, WKBArray,
};
use crate::error::{GeoArrowError, Result};
use crate::geo_traits::{
    CoordTrait, GeometryTrait, GeometryType, LineStringTrait, MultiLineStringTrait,
};
use crate::io::wkb::reader::WKBLineString;
use crate::scalar::WKB;
use crate::trait_::{ArrayAccessor, GeometryArrayBuilder, IntoArrow};
use arrow_array::{Array, GenericListArray, OffsetSizeTrait};
use arrow_buffer::NullBufferBuilder;
use std::convert::From;
use std::sync::Arc;

/// The GeoArrow equivalent to `Vec<Option<LineString>>`: a mutable collection of LineStrings.
///
/// Converting an [`LineStringBuilder`] into a [`LineStringArray`] is `O(1)`.
#[derive(Debug)]
pub struct LineStringBuilder<const D: usize> {
    metadata: Arc<ArrayMetadata>,

    pub(crate) coords: CoordBufferBuilder<D>,

    /// Offsets into the coordinate array where each geometry starts
    pub(crate) geom_offsets: OffsetsBuilder<i32>,

    /// Validity is only defined at the geometry level
    pub(crate) validity: NullBufferBuilder,
}

impl<const D: usize> LineStringBuilder<D> {
    /// Creates a new empty [`LineStringBuilder`].
    pub fn new() -> Self {
        Self::new_with_options(Default::default(), Default::default())
    }

    pub fn new_with_options(coord_type: CoordType, metadata: Arc<ArrayMetadata>) -> Self {
        Self::with_capacity_and_options(Default::default(), coord_type, metadata)
    }

    /// Creates a new [`LineStringBuilder`] with a capacity.
    pub fn with_capacity(capacity: LineStringCapacity) -> Self {
        Self::with_capacity_and_options(capacity, Default::default(), Default::default())
    }

    pub fn with_capacity_and_options(
        capacity: LineStringCapacity,
        coord_type: CoordType,
        metadata: Arc<ArrayMetadata>,
    ) -> Self {
        let coords = match coord_type {
            CoordType::Interleaved => CoordBufferBuilder::Interleaved(
                InterleavedCoordBufferBuilder::with_capacity(capacity.coord_capacity()),
            ),
            CoordType::Separated => CoordBufferBuilder::Separated(
                SeparatedCoordBufferBuilder::with_capacity(capacity.coord_capacity()),
            ),
        };
        Self {
            coords,
            geom_offsets: OffsetsBuilder::with_capacity(capacity.geom_capacity()),
            validity: NullBufferBuilder::new(capacity.geom_capacity()),
            metadata,
        }
    }

    /// Reserves capacity for at least `additional` more LineStrings to be inserted
    /// in the given `Vec<T>`. The collection may reserve more space to
    /// speculatively avoid frequent reallocations. After calling `reserve`,
    /// capacity will be greater than or equal to `self.len() + additional`.
    /// Does nothing if capacity is already sufficient.
    pub fn reserve(&mut self, additional: LineStringCapacity) {
        self.coords.reserve(additional.coord_capacity());
        self.geom_offsets.reserve(additional.geom_capacity());
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
        self.coords.reserve_exact(additional.coord_capacity());
        self.geom_offsets.reserve_exact(additional.geom_capacity());
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
        coords: CoordBufferBuilder<D>,
        geom_offsets: OffsetsBuilder<i32>,
        validity: NullBufferBuilder,
        metadata: Arc<ArrayMetadata>,
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
            metadata,
        })
    }

    /// Extract the low-level APIs from the [`LineStringBuilder`].
    pub fn into_inner(
        self,
    ) -> (
        CoordBufferBuilder<D>,
        OffsetsBuilder<i32>,
        NullBufferBuilder,
    ) {
        (self.coords, self.geom_offsets, self.validity)
    }

    /// Needs to be called when a valid value was extended to this array.
    /// This is a relatively low level function, prefer `try_push` when you can.
    #[inline]
    pub(crate) fn try_push_length(&mut self, geom_offsets_length: usize) -> Result<()> {
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

    pub fn finish(self) -> LineStringArray<D> {
        self.into()
    }

    pub fn with_capacity_from_iter<'a>(
        geoms: impl Iterator<Item = Option<&'a (impl LineStringTrait + 'a)>>,
    ) -> Self {
        Self::with_capacity_and_options_from_iter(geoms, Default::default(), Default::default())
    }

    pub fn with_capacity_and_options_from_iter<'a>(
        geoms: impl Iterator<Item = Option<&'a (impl LineStringTrait + 'a)>>,
        coord_type: CoordType,
        metadata: Arc<ArrayMetadata>,
    ) -> Self {
        let counter = LineStringCapacity::from_line_strings(geoms);
        Self::with_capacity_and_options(counter, coord_type, metadata)
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

    pub fn from_line_strings(
        geoms: &[impl LineStringTrait<T = f64>],
        coord_type: Option<CoordType>,
        metadata: Arc<ArrayMetadata>,
    ) -> Self {
        let mut array = Self::with_capacity_and_options_from_iter(
            geoms.iter().map(Some),
            coord_type.unwrap_or_default(),
            metadata,
        );
        array.extend_from_iter(geoms.iter().map(Some));
        array
    }

    pub fn from_nullable_line_strings(
        geoms: &[Option<impl LineStringTrait<T = f64>>],
        coord_type: Option<CoordType>,
        metadata: Arc<ArrayMetadata>,
    ) -> Self {
        let mut array = Self::with_capacity_and_options_from_iter(
            geoms.iter().map(|x| x.as_ref()),
            coord_type.unwrap_or_default(),
            metadata,
        );
        array.extend_from_iter(geoms.iter().map(|x| x.as_ref()));
        array
    }

    /// Add a new LineString to the end of this array.
    ///
    /// # Errors
    ///
    /// This function errors iff the new last item is larger than what O supports.
    #[inline]
    pub fn push_line_string(
        &mut self,
        value: Option<&impl LineStringTrait<T = f64>>,
    ) -> Result<()> {
        if let Some(line_string) = value {
            let num_coords = line_string.num_coords();
            for coord in line_string.coords() {
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
    #[inline]
    pub unsafe fn push_coord(&mut self, coord: &impl CoordTrait<T = f64>) {
        self.coords.push_coord(coord)
    }

    #[inline]
    pub fn push_geometry(&mut self, value: Option<&impl GeometryTrait<T = f64>>) -> Result<()> {
        if let Some(value) = value {
            match value.as_type() {
                GeometryType::LineString(g) => self.push_line_string(Some(g))?,
                GeometryType::MultiLineString(ml) => {
                    if ml.num_line_strings() == 1 {
                        self.push_line_string(Some(&ml.line_string(0).unwrap()))?
                    } else {
                        return Err(GeoArrowError::General("Incorrect type".to_string()));
                    }
                }
                _ => return Err(GeoArrowError::General("Incorrect type".to_string())),
            }
        } else {
            self.push_null();
        };
        Ok(())
    }

    pub(crate) fn from_wkb<W: OffsetSizeTrait>(
        wkb_objects: &[Option<WKB<'_, W>>],
        coord_type: Option<CoordType>,
        metadata: Arc<ArrayMetadata>,
    ) -> Result<Self> {
        let wkb_objects2: Vec<Option<WKBLineString>> = wkb_objects
            .iter()
            .map(|maybe_wkb| {
                maybe_wkb
                    .as_ref()
                    .map(|wkb| wkb.to_wkb_object().into_line_string())
            })
            .collect();
        Ok(Self::from_nullable_line_strings(
            &wkb_objects2,
            coord_type,
            metadata,
        ))
    }
}

impl<const D: usize> GeometryArrayBuilder for LineStringBuilder<D> {
    fn new() -> Self {
        Self::new()
    }

    fn with_geom_capacity_and_options(
        geom_capacity: usize,
        coord_type: CoordType,
        metadata: Arc<ArrayMetadata>,
    ) -> Self {
        let capacity = LineStringCapacity::new(0, geom_capacity);
        Self::with_capacity_and_options(capacity, coord_type, metadata)
    }

    fn push_geometry(&mut self, value: Option<&impl GeometryTrait<T = f64>>) -> Result<()> {
        self.push_geometry(value)
    }

    fn finish(self) -> Arc<dyn crate::NativeArray> {
        Arc::new(self.finish())
    }

    fn len(&self) -> usize {
        self.geom_offsets.len_proxy()
    }

    fn nulls(&self) -> &NullBufferBuilder {
        &self.validity
    }

    fn into_array_ref(self) -> Arc<dyn Array> {
        Arc::new(self.into_arrow())
    }

    fn coord_type(&self) -> CoordType {
        self.coords.coord_type()
    }

    fn set_metadata(&mut self, metadata: Arc<ArrayMetadata>) {
        self.metadata = metadata;
    }

    fn metadata(&self) -> Arc<ArrayMetadata> {
        self.metadata.clone()
    }
}

impl<const D: usize> IntoArrow for LineStringBuilder<D> {
    type ArrowArray = GenericListArray<i32>;

    fn into_arrow(self) -> Self::ArrowArray {
        let linestring_arr: LineStringArray<D> = self.into();
        linestring_arr.into_arrow()
    }
}

impl<const D: usize> Default for LineStringBuilder<D> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const D: usize> From<LineStringBuilder<D>> for LineStringArray<D> {
    fn from(mut other: LineStringBuilder<D>) -> Self {
        let validity = other.validity.finish();
        Self::new(
            other.coords.into(),
            other.geom_offsets.into(),
            validity,
            other.metadata,
        )
    }
}

impl<const D: usize> From<LineStringBuilder<D>> for GenericListArray<i32> {
    fn from(arr: LineStringBuilder<D>) -> Self {
        arr.into_arrow()
    }
}

impl<G: LineStringTrait<T = f64>, const D: usize> From<&[G]> for LineStringBuilder<D> {
    fn from(geoms: &[G]) -> Self {
        Self::from_line_strings(geoms, Default::default(), Default::default())
    }
}

impl<G: LineStringTrait<T = f64>, const D: usize> From<Vec<Option<G>>> for LineStringBuilder<D> {
    fn from(geoms: Vec<Option<G>>) -> Self {
        Self::from_nullable_line_strings(&geoms, Default::default(), Default::default())
    }
}

impl<O: OffsetSizeTrait, const D: usize> TryFrom<WKBArray<O>> for LineStringBuilder<D> {
    type Error = GeoArrowError;

    fn try_from(value: WKBArray<O>) -> Result<Self> {
        let metadata = value.metadata.clone();
        let wkb_objects: Vec<Option<WKB<'_, O>>> = value.iter().collect();
        Self::from_wkb(&wkb_objects, Default::default(), metadata)
    }
}

/// LineString and MultiPoint have the same layout, so enable conversions between the two to change
/// the semantic type
impl<const D: usize> From<LineStringBuilder<D>> for MultiPointBuilder<D> {
    fn from(value: LineStringBuilder<D>) -> Self {
        Self::try_new(
            value.coords,
            value.geom_offsets,
            value.validity,
            value.metadata,
        )
        .unwrap()
    }
}
