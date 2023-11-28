use std::sync::Arc;

// use super::array::check;
use crate::array::mutable_offset::OffsetsBuilder;
use crate::array::{
    CoordType, MultiPolygonArray, MutableCoordBuffer, MutableInterleavedCoordBuffer,
    MutableSeparatedCoordBuffer, WKBArray,
};
use crate::error::{GeoArrowError, Result};
use crate::geo_traits::{LineStringTrait, MultiPolygonTrait, PolygonTrait};
use crate::io::wkb::reader::maybe_multipolygon::WKBMaybeMultiPolygon;
use crate::scalar::WKB;
use crate::trait_::IntoArrow;
use arrow_array::{Array, GenericListArray, OffsetSizeTrait};
use arrow_buffer::{NullBufferBuilder, OffsetBuffer};

pub type MutableMultiPolygonParts<O> = (
    MutableCoordBuffer,
    OffsetsBuilder<O>,
    OffsetsBuilder<O>,
    OffsetsBuilder<O>,
    NullBufferBuilder,
);

/// The Arrow equivalent to `Vec<Option<MultiPolygon>>`.
/// Converting a [`MutableMultiPolygonArray`] into a [`MultiPolygonArray`] is `O(1)`.
#[derive(Debug)]
pub struct MutableMultiPolygonArray<O: OffsetSizeTrait> {
    pub(crate) coords: MutableCoordBuffer,

    /// OffsetsBuilder into the polygon array where each geometry starts
    pub(crate) geom_offsets: OffsetsBuilder<O>,

    /// OffsetsBuilder into the ring array where each polygon starts
    pub(crate) polygon_offsets: OffsetsBuilder<O>,

    /// OffsetsBuilder into the coordinate array where each ring starts
    pub(crate) ring_offsets: OffsetsBuilder<O>,

    /// Validity is only defined at the geometry level
    pub(crate) validity: NullBufferBuilder,
}

impl<O: OffsetSizeTrait> MutableMultiPolygonArray<O> {
    /// Creates a new empty [`MutableMultiPolygonArray`].
    pub fn new() -> Self {
        Self::new_with_options(Default::default())
    }

    pub fn new_with_options(coord_type: CoordType) -> Self {
        Self::with_capacities_and_options(0, 0, 0, 0, coord_type)
    }

    /// Creates a new [`MutableMultiPolygonArray`] with a capacity.
    pub fn with_capacities(
        coord_capacity: usize,
        ring_capacity: usize,
        polygon_capacity: usize,
        geom_capacity: usize,
    ) -> Self {
        Self::with_capacities_and_options(
            coord_capacity,
            ring_capacity,
            polygon_capacity,
            geom_capacity,
            Default::default(),
        )
    }

    pub fn with_capacities_and_options(
        coord_capacity: usize,
        ring_capacity: usize,
        polygon_capacity: usize,
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
            polygon_offsets: OffsetsBuilder::with_capacity(polygon_capacity),
            ring_offsets: OffsetsBuilder::with_capacity(ring_capacity),
            validity: NullBufferBuilder::new(geom_capacity),
        }
    }

    pub fn with_capacities_from_iter<'a>(
        geoms: impl Iterator<Item = Option<&'a (impl MultiPolygonTrait + 'a)>>,
    ) -> Self {
        Self::with_capacities_and_options_from_iter(geoms, Default::default())
    }

    pub fn with_capacities_and_options_from_iter<'a>(
        geoms: impl Iterator<Item = Option<&'a (impl MultiPolygonTrait + 'a)>>,
        coord_type: CoordType,
    ) -> Self {
        let (coord_capacity, ring_capacity, polygon_capacity, geom_capacity) =
            count_from_iter(geoms);

        Self::with_capacities_and_options(
            coord_capacity,
            ring_capacity,
            polygon_capacity,
            geom_capacity,
            coord_type,
        )
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
        polygon_additional: usize,
        geom_additional: usize,
    ) {
        self.coords.reserve(coord_additional);
        self.ring_offsets.reserve(ring_additional);
        self.polygon_offsets.reserve(polygon_additional);
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
        polygon_additional: usize,
        geom_additional: usize,
    ) {
        self.coords.reserve_exact(coord_additional);
        self.ring_offsets.reserve(ring_additional);
        self.polygon_offsets.reserve(polygon_additional);
        self.geom_offsets.reserve(geom_additional);
    }

    pub fn reserve_from_iter<'a>(
        &mut self,
        geoms: impl Iterator<Item = Option<&'a (impl MultiPolygonTrait + 'a)>>,
    ) {
        let (coord_capacity, ring_capacity, polygon_capacity, geom_capacity) =
            count_from_iter(geoms);
        self.reserve(
            coord_capacity,
            ring_capacity,
            polygon_capacity,
            geom_capacity,
        )
    }

    pub fn reserve_exact_from_iter<'a>(
        &mut self,
        geoms: impl Iterator<Item = Option<&'a (impl MultiPolygonTrait + 'a)>>,
    ) {
        let (coord_capacity, ring_capacity, polygon_capacity, geom_capacity) =
            count_from_iter(geoms);
        self.reserve_exact(
            coord_capacity,
            ring_capacity,
            polygon_capacity,
            geom_capacity,
        )
    }

    /// The canonical method to create a [`MutableMultiPolygonArray`] out of its internal
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
    /// - if the largest polygon offset does not match the size of ring offsets
    /// - if the largest geometry offset does not match the size of polygon offsets
    pub fn try_new(
        coords: MutableCoordBuffer,
        geom_offsets: OffsetsBuilder<O>,
        polygon_offsets: OffsetsBuilder<O>,
        ring_offsets: OffsetsBuilder<O>,
        validity: NullBufferBuilder,
    ) -> Result<Self> {
        // check(
        //     &coords.clone().into(),
        //     &geom_offsets.clone().into(),
        //     &polygon_offsets.clone().into(),
        //     &ring_offsets.clone().into(),
        //     validity.as_ref().map(|x| x.len()),
        // )?;
        Ok(Self {
            coords,
            geom_offsets,
            polygon_offsets,
            ring_offsets,
            validity,
        })
    }

    /// Extract the low-level APIs from the [`MutableMultiPolygonArray`].
    pub fn into_inner(self) -> MutableMultiPolygonParts<O> {
        (
            self.coords,
            self.geom_offsets,
            self.polygon_offsets,
            self.ring_offsets,
            self.validity,
        )
    }

    pub fn into_array_ref(self) -> Arc<dyn Array> {
        Arc::new(self.into_arrow())
    }

    /// Add a new Polygon to the end of this array.
    ///
    /// # Errors
    ///
    /// This function errors iff the new last item is larger than what O supports.
    pub fn push_polygon(&mut self, value: Option<&impl PolygonTrait<T = f64>>) -> Result<()> {
        if let Some(polygon) = value {
            let exterior_ring = polygon.exterior();
            if exterior_ring.is_none() {
                self.push_empty();
                return Ok(());
            }

            // Total number of polygons in this MultiPolygon
            let num_polygons = 1;
            self.geom_offsets.try_push_usize(num_polygons).unwrap();

            let ext_ring = polygon.exterior().unwrap();
            for coord_idx in 0..ext_ring.num_coords() {
                let coord = ext_ring.coord(coord_idx).unwrap();
                self.coords.push_coord(coord);
            }

            // Total number of rings in this Multipolygon
            self.polygon_offsets
                .try_push_usize(polygon.num_interiors() + 1)
                .unwrap();

            // Number of coords for each ring
            self.ring_offsets
                .try_push_usize(ext_ring.num_coords())
                .unwrap();

            for int_ring_idx in 0..polygon.num_interiors() {
                let int_ring = polygon.interior(int_ring_idx).unwrap();
                self.ring_offsets
                    .try_push_usize(int_ring.num_coords())
                    .unwrap();

                for coord_idx in 0..int_ring.num_coords() {
                    let coord = int_ring.coord(coord_idx).unwrap();
                    self.coords.push_coord(coord);
                }
            }
        } else {
            self.push_null();
        };
        Ok(())
    }

    /// Add a new MultiPolygon to the end of this array.
    ///
    /// # Errors
    ///
    /// This function errors iff the new last item is larger than what O supports.
    pub fn push_multi_polygon(
        &mut self,
        value: Option<&impl MultiPolygonTrait<T = f64>>,
    ) -> Result<()> {
        if let Some(multi_polygon) = value {
            // Total number of polygons in this MultiPolygon
            let num_polygons = multi_polygon.num_polygons();
            unsafe { self.try_push_geom_offset(num_polygons)? }

            // Iterate over polygons
            for polygon_idx in 0..num_polygons {
                let polygon = multi_polygon.polygon(polygon_idx).unwrap();

                // Here we unwrap the exterior ring because a polygon inside a multi polygon should
                // never be empty.
                let ext_ring = polygon.exterior().unwrap();
                for coord_idx in 0..ext_ring.num_coords() {
                    let coord = ext_ring.coord(coord_idx).unwrap();
                    self.coords.push_coord(coord);
                }

                // Total number of rings in this Multipolygon
                self.polygon_offsets
                    .try_push_usize(polygon.num_interiors() + 1)
                    .unwrap();

                // Number of coords for each ring
                self.ring_offsets
                    .try_push_usize(ext_ring.num_coords())
                    .unwrap();

                for int_ring_idx in 0..polygon.num_interiors() {
                    let int_ring = polygon.interior(int_ring_idx).unwrap();
                    self.ring_offsets
                        .try_push_usize(int_ring.num_coords())
                        .unwrap();

                    for coord_idx in 0..int_ring.num_coords() {
                        let coord = int_ring.coord(coord_idx).unwrap();
                        self.coords.push_coord(coord);
                    }
                }
            }
        } else {
            self.push_null();
        };
        Ok(())
    }

    pub fn extend_from_iter<'a>(
        &mut self,
        geoms: impl Iterator<Item = Option<&'a (impl MultiPolygonTrait<T = f64> + 'a)>>,
    ) {
        geoms
            .into_iter()
            .try_for_each(|maybe_multi_polygon| self.push_multi_polygon(maybe_multi_polygon))
            .unwrap();
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

    /// Push a raw offset to the underlying polygon offsets buffer.
    ///
    /// # Safety
    ///
    /// This is marked as unsafe because care must be taken to ensure that pushing raw offsets
    /// upholds the necessary invariants of the array.
    pub unsafe fn try_push_polygon_offset(&mut self, offsets_length: usize) -> Result<()> {
        self.polygon_offsets.try_push_usize(offsets_length)?;
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
    pub(crate) fn push_empty(&mut self) {
        self.geom_offsets.try_push_usize(0).unwrap();
        self.validity.append(true);
    }

    #[inline]
    pub(crate) fn push_null(&mut self) {
        // NOTE! Only the geom_offsets array needs to get extended, because the next geometry will
        // point to the same polygon array location
        // Note that we don't use self.try_push_geom_offset because that sets validity to true
        self.geom_offsets.extend_constant(1);
        self.validity.append(false);
    }

    pub fn from_multi_polygons(
        geoms: &[impl MultiPolygonTrait<T = f64>],
        coord_type: Option<CoordType>,
    ) -> Self {
        let mut array = Self::with_capacities_and_options_from_iter(
            geoms.iter().map(Some),
            coord_type.unwrap_or_default(),
        );
        array.extend_from_iter(geoms.iter().map(Some));
        array
    }

    pub fn from_nullable_multi_polygons(
        geoms: &[Option<impl MultiPolygonTrait<T = f64>>],
        coord_type: Option<CoordType>,
    ) -> Self {
        let mut array = Self::with_capacities_and_options_from_iter(
            geoms.iter().map(|x| x.as_ref()),
            coord_type.unwrap_or_default(),
        );
        array.extend_from_iter(geoms.iter().map(|x| x.as_ref()));
        array
    }
}

impl<O: OffsetSizeTrait> Default for MutableMultiPolygonArray<O> {
    fn default() -> Self {
        Self::new()
    }
}

impl<O: OffsetSizeTrait> IntoArrow for MutableMultiPolygonArray<O> {
    type ArrowArray = GenericListArray<O>;

    fn into_arrow(self) -> Self::ArrowArray {
        let arr: MultiPolygonArray<O> = self.into();
        arr.into_arrow()
    }
}

impl<O: OffsetSizeTrait> From<MutableMultiPolygonArray<O>> for MultiPolygonArray<O> {
    fn from(other: MutableMultiPolygonArray<O>) -> Self {
        let validity = other.validity.finish_cloned();

        let geom_offsets: OffsetBuffer<O> = other.geom_offsets.into();
        let polygon_offsets: OffsetBuffer<O> = other.polygon_offsets.into();
        let ring_offsets: OffsetBuffer<O> = other.ring_offsets.into();

        Self::new(
            other.coords.into(),
            geom_offsets,
            polygon_offsets,
            ring_offsets,
            validity,
        )
    }
}

fn count_from_iter<'a>(
    geoms: impl Iterator<Item = Option<&'a (impl MultiPolygonTrait + 'a)>>,
) -> (usize, usize, usize, usize) {
    let mut coord_capacity = 0;
    let mut ring_capacity = 0;
    let mut polygon_capacity = 0;
    let mut geom_capacity = 0;

    for maybe_multi_polygon in geoms.into_iter() {
        geom_capacity += 1;

        if let Some(multi_polygon) = maybe_multi_polygon {
            // Total number of polygons in this MultiPolygon
            let num_polygons = multi_polygon.num_polygons();
            polygon_capacity += num_polygons;

            for polygon_idx in 0..num_polygons {
                let polygon = multi_polygon.polygon(polygon_idx).unwrap();

                // Total number of rings in this MultiPolygon
                ring_capacity += polygon.num_interiors() + 1;

                // Number of coords for each ring
                if let Some(exterior) = polygon.exterior() {
                    coord_capacity += exterior.num_coords();
                }

                for int_ring_idx in 0..polygon.num_interiors() {
                    let int_ring = polygon.interior(int_ring_idx).unwrap();
                    coord_capacity += int_ring.num_coords();
                }
            }
        }
    }

    (
        coord_capacity,
        ring_capacity,
        polygon_capacity,
        geom_capacity,
    )
}

impl<O: OffsetSizeTrait, G: MultiPolygonTrait<T = f64>> From<Vec<G>>
    for MutableMultiPolygonArray<O>
{
    fn from(geoms: Vec<G>) -> Self {
        Self::from_multi_polygons(&geoms, Default::default())
    }
}

impl<O: OffsetSizeTrait, G: MultiPolygonTrait<T = f64>> From<Vec<Option<G>>>
    for MutableMultiPolygonArray<O>
{
    fn from(geoms: Vec<Option<G>>) -> Self {
        Self::from_nullable_multi_polygons(&geoms, Default::default())
    }
}

impl<O: OffsetSizeTrait, G: MultiPolygonTrait<T = f64>> From<bumpalo::collections::Vec<'_, G>>
    for MutableMultiPolygonArray<O>
{
    fn from(geoms: bumpalo::collections::Vec<'_, G>) -> Self {
        Self::from_multi_polygons(&geoms, Default::default())
    }
}

impl<O: OffsetSizeTrait, G: MultiPolygonTrait<T = f64>>
    From<bumpalo::collections::Vec<'_, Option<G>>> for MutableMultiPolygonArray<O>
{
    fn from(geoms: bumpalo::collections::Vec<'_, Option<G>>) -> Self {
        Self::from_nullable_multi_polygons(&geoms, Default::default())
    }
}

impl<O: OffsetSizeTrait> TryFrom<WKBArray<O>> for MutableMultiPolygonArray<O> {
    type Error = GeoArrowError;

    fn try_from(value: WKBArray<O>) -> Result<Self> {
        let wkb_objects: Vec<Option<WKB<'_, O>>> = value.iter().collect();
        let wkb_objects2: Vec<Option<WKBMaybeMultiPolygon>> = wkb_objects
            .iter()
            .map(|maybe_wkb| {
                maybe_wkb
                    .as_ref()
                    .map(|wkb| wkb.to_wkb_object().into_maybe_multi_polygon())
            })
            .collect();
        Ok(wkb_objects2.into())
    }
}
