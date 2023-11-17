use std::sync::Arc;

// use super::array::check;
use crate::array::mutable_offset::OffsetsBuilder;
use crate::array::{
    MutableCoordBuffer, MutableInterleavedCoordBuffer, MutableMultiLineStringArray, PolygonArray,
    WKBArray,
};
use crate::error::{GeoArrowError, Result};
use crate::geo_traits::{CoordTrait, LineStringTrait, PolygonTrait};
use crate::io::wkb::reader::polygon::WKBPolygon;
use crate::scalar::WKB;
use crate::trait_::{GeometryArrayTrait, IntoArrow};
use arrow_array::{Array, GenericListArray, OffsetSizeTrait};
use arrow_buffer::{NullBufferBuilder, OffsetBuffer};

pub type MutablePolygonParts<O> = (
    MutableCoordBuffer,
    OffsetsBuilder<O>,
    OffsetsBuilder<O>,
    NullBufferBuilder,
);

/// The Arrow equivalent to `Vec<Option<Polygon>>`.
/// Converting a [`MutablePolygonArray`] into a [`PolygonArray`] is `O(1)`.
#[derive(Debug)]
pub struct MutablePolygonArray<O: OffsetSizeTrait> {
    pub(crate) coords: MutableCoordBuffer,

    /// OffsetsBuilder into the ring array where each geometry starts
    pub(crate) geom_offsets: OffsetsBuilder<O>,

    /// OffsetsBuilder into the coordinate array where each ring starts
    pub(crate) ring_offsets: OffsetsBuilder<O>,

    /// Validity is only defined at the geometry level
    pub(crate) validity: NullBufferBuilder,
}

impl<O: OffsetSizeTrait> MutablePolygonArray<O> {
    /// Creates a new empty [`MutablePolygonArray`].
    pub fn new() -> Self {
        Self::with_capacities(0, 0, 0)
    }

    /// Creates a new [`MutablePolygonArray`] with given capacities and no validity.
    pub fn with_capacities(
        coord_capacity: usize,
        ring_capacity: usize,
        geom_capacity: usize,
    ) -> Self {
        let coords = MutableInterleavedCoordBuffer::with_capacity(coord_capacity);
        Self {
            coords: MutableCoordBuffer::Interleaved(coords),
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

    /// The canonical method to create a [`MutablePolygonArray`] out of its internal components.
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

    /// Extract the low-level APIs from the [`MutablePolygonArray`].
    pub fn into_inner(self) -> MutablePolygonParts<O> {
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

            // - Get exterior ring
            // - Add exterior ring's # of coords self.ring_offsets
            // - Push ring's coords to self.coords
            let ext_ring = polygon.exterior().unwrap();
            let ext_ring_num_coords = ext_ring.num_coords();
            self.ring_offsets.try_push_usize(ext_ring_num_coords)?;
            for coord_idx in 0..ext_ring_num_coords {
                let coord = ext_ring.coord(coord_idx).unwrap();
                self.coords.push_xy(coord.x(), coord.y());
            }

            // Total number of rings in this polygon
            let num_interiors = polygon.num_interiors();
            self.geom_offsets.try_push_usize(num_interiors + 1)?;

            // For each interior ring:
            // - Get ring
            // - Add ring's # of coords to self.ring_offsets
            // - Push ring's coords to self.coords
            for int_ring_idx in 0..num_interiors {
                let int_ring = polygon.interior(int_ring_idx).unwrap();
                let int_ring_num_coords = int_ring.num_coords();
                self.ring_offsets.try_push_usize(int_ring_num_coords)?;
                for coord_idx in 0..int_ring_num_coords {
                    let coord = int_ring.coord(coord_idx).unwrap();
                    self.coords.push_xy(coord.x(), coord.y());
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
    pub(crate) fn push_empty(&mut self) {
        self.geom_offsets.try_push_usize(0).unwrap();
        self.validity.append(true);
    }

    #[inline]
    pub(crate) fn push_null(&mut self) {
        // NOTE! Only the geom_offsets array needs to get extended, because the next geometry will
        // point to the same ring array location
        self.geom_offsets.extend_constant(1);
        self.validity.append(false);
    }
}

impl<O: OffsetSizeTrait> Default for MutablePolygonArray<O> {
    fn default() -> Self {
        Self::new()
    }
}

impl<O: OffsetSizeTrait> IntoArrow for MutablePolygonArray<O> {
    type ArrowArray = GenericListArray<O>;

    fn into_arrow(self) -> Self::ArrowArray {
        let polygon_array: PolygonArray<O> = self.into();
        polygon_array.into_arrow()
    }
}

impl<O: OffsetSizeTrait> From<MutablePolygonArray<O>> for PolygonArray<O> {
    fn from(other: MutablePolygonArray<O>) -> Self {
        let validity = other.validity.finish_cloned();

        let geom_offsets: OffsetBuffer<O> = other.geom_offsets.into();
        let ring_offsets: OffsetBuffer<O> = other.ring_offsets.into();

        Self::new(other.coords.into(), geom_offsets, ring_offsets, validity)
    }
}

fn first_pass<'a>(
    geoms: impl Iterator<Item = Option<impl PolygonTrait + 'a>>,
    geoms_length: usize,
) -> (usize, usize, usize) {
    // Total number of coordinates
    let mut coord_capacity = 0;
    let mut ring_capacity = 0;
    let geom_capacity = geoms_length;

    for polygon in geoms.into_iter().flatten() {
        // Total number of rings in this polygon
        let num_interiors = polygon.num_interiors();
        ring_capacity += num_interiors + 1;

        // Number of coords for each ring
        if let Some(exterior) = polygon.exterior() {
            coord_capacity += exterior.num_coords();
        }

        for int_ring_idx in 0..polygon.num_interiors() {
            let int_ring = polygon.interior(int_ring_idx).unwrap();
            coord_capacity += int_ring.num_coords();
        }
    }

    // TODO: dataclass for capacities to access them by name?
    (coord_capacity, ring_capacity, geom_capacity)
}

fn second_pass<'a, O: OffsetSizeTrait>(
    geoms: impl Iterator<Item = Option<impl PolygonTrait<T = f64> + 'a>>,
    coord_capacity: usize,
    ring_capacity: usize,
    geom_capacity: usize,
) -> MutablePolygonArray<O> {
    let mut array =
        MutablePolygonArray::with_capacities(coord_capacity, ring_capacity, geom_capacity);

    geoms
        .into_iter()
        .try_for_each(|maybe_polygon| array.push_polygon(maybe_polygon.as_ref()))
        .unwrap();

    array
}

impl<O: OffsetSizeTrait> From<Vec<geo::Polygon>> for MutablePolygonArray<O> {
    fn from(geoms: Vec<geo::Polygon>) -> Self {
        let (coord_capacity, ring_capacity, geom_capacity) =
            first_pass(geoms.iter().map(Some), geoms.len());
        second_pass(
            geoms.into_iter().map(Some),
            coord_capacity,
            ring_capacity,
            geom_capacity,
        )
    }
}

impl<O: OffsetSizeTrait> From<Vec<Option<geo::Polygon>>> for MutablePolygonArray<O> {
    fn from(geoms: Vec<Option<geo::Polygon>>) -> Self {
        let (coord_capacity, ring_capacity, geom_capacity) =
            first_pass(geoms.iter().map(|x| x.as_ref()), geoms.len());
        second_pass(
            geoms.into_iter(),
            coord_capacity,
            ring_capacity,
            geom_capacity,
        )
    }
}

impl<O: OffsetSizeTrait> From<bumpalo::collections::Vec<'_, geo::Polygon>>
    for MutablePolygonArray<O>
{
    fn from(geoms: bumpalo::collections::Vec<'_, geo::Polygon>) -> Self {
        let (coord_capacity, ring_capacity, geom_capacity) =
            first_pass(geoms.iter().map(Some), geoms.len());
        second_pass(
            geoms.into_iter().map(Some),
            coord_capacity,
            ring_capacity,
            geom_capacity,
        )
    }
}
impl<O: OffsetSizeTrait> From<bumpalo::collections::Vec<'_, Option<geo::Polygon>>>
    for MutablePolygonArray<O>
{
    fn from(geoms: bumpalo::collections::Vec<'_, Option<geo::Polygon>>) -> Self {
        let (coord_capacity, ring_capacity, geom_capacity) =
            first_pass(geoms.iter().map(|x| x.as_ref()), geoms.len());
        second_pass(
            geoms.into_iter(),
            coord_capacity,
            ring_capacity,
            geom_capacity,
        )
    }
}

impl<O: OffsetSizeTrait> TryFrom<WKBArray<O>> for MutablePolygonArray<O> {
    type Error = GeoArrowError;

    fn try_from(value: WKBArray<O>) -> Result<Self> {
        let wkb_objects: Vec<Option<WKB<'_, O>>> = value.iter().collect();
        let wkb_objects2: Vec<Option<WKBPolygon>> = wkb_objects
            .iter()
            .map(|maybe_wkb| {
                maybe_wkb
                    .as_ref()
                    .map(|wkb| wkb.to_wkb_object().into_polygon())
            })
            .collect();
        let (coord_capacity, ring_capacity, geom_capacity) =
            first_pass(wkb_objects2.iter().map(|item| item.as_ref()), value.len());
        Ok(second_pass(
            wkb_objects2.iter().map(|item| item.as_ref()),
            coord_capacity,
            ring_capacity,
            geom_capacity,
        ))
    }
}

/// Polygon and MultiLineString have the same layout, so enable conversions between the two to
/// change the semantic type
impl<O: OffsetSizeTrait> From<MutablePolygonArray<O>> for MutableMultiLineStringArray<O> {
    fn from(value: MutablePolygonArray<O>) -> Self {
        Self::try_new(
            value.coords,
            value.geom_offsets,
            value.ring_offsets,
            value.validity,
        )
        .unwrap()
    }
}
