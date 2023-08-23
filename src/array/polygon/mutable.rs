use super::array::check;
use crate::array::{
    MutableCoordBuffer, MutableInterleavedCoordBuffer, MutableMultiLineStringArray, PolygonArray,
    WKBArray,
};
use crate::error::{GeoArrowError, Result};
use crate::geo_traits::{CoordTrait, LineStringTrait, PolygonTrait};
use crate::io::wkb::reader::polygon::WKBPolygon;
use crate::scalar::WKB;
use crate::trait_::GeometryArrayTrait;
use arrow2::array::{Array, ListArray};
use arrow2::bitmap::{Bitmap, MutableBitmap};
use arrow2::offset::{Offsets, OffsetsBuffer};
use arrow2::types::Offset;

pub type MutablePolygonParts<O> = (
    MutableCoordBuffer,
    Offsets<O>,
    Offsets<O>,
    Option<MutableBitmap>,
);

/// The Arrow equivalent to `Vec<Option<Polygon>>`.
/// Converting a [`MutablePolygonArray`] into a [`PolygonArray`] is `O(1)`.
#[derive(Debug, Clone)]
pub struct MutablePolygonArray<O: Offset> {
    coords: MutableCoordBuffer,

    /// Offsets into the ring array where each geometry starts
    geom_offsets: Offsets<O>,

    /// Offsets into the coordinate array where each ring starts
    ring_offsets: Offsets<O>,

    /// Validity is only defined at the geometry level
    validity: Option<MutableBitmap>,
}

impl<'a, O: Offset> MutablePolygonArray<O> {
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
            geom_offsets: Offsets::<O>::with_capacity(geom_capacity),
            ring_offsets: Offsets::<O>::with_capacity(ring_capacity),
            validity: None,
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
        if let Some(validity) = self.validity.as_mut() {
            validity.reserve(geom_additional)
        }
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
        if let Some(validity) = self.validity.as_mut() {
            validity.reserve(geom_additional)
        }
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
        geom_offsets: Offsets<O>,
        ring_offsets: Offsets<O>,
        validity: Option<MutableBitmap>,
    ) -> Result<Self> {
        check(
            &coords.clone().into(),
            &geom_offsets.clone().into(),
            &ring_offsets.clone().into(),
            validity.as_ref().map(|x| x.len()),
        )?;
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

    pub fn into_arrow(self) -> ListArray<O> {
        let polygon_array: PolygonArray<O> = self.into();
        polygon_array.into_arrow()
    }

    pub fn into_boxed_arrow(self) -> Box<dyn Array> {
        self.into_arrow().boxed()
    }

    /// Add a new Polygon to the end of this array.
    ///
    /// # Errors
    ///
    /// This function errors iff the new last item is larger than what O supports.
    pub fn push_polygon(&mut self, value: Option<&impl PolygonTrait<'a, T = f64>>) -> Result<()> {
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
            for int_ring_idx in 0..polygon.num_interiors() {
                let int_ring = polygon.interior(int_ring_idx).unwrap();
                let int_ring_num_coords = int_ring.num_coords();
                self.ring_offsets.try_push_usize(int_ring_num_coords)?;
                for coord_idx in 0..int_ring_num_coords {
                    let coord = int_ring.coord(coord_idx).unwrap();
                    self.coords.push_xy(coord.x(), coord.y());
                }
            }

            // Set validity to true if validity buffer exists
            if let Some(validity) = &mut self.validity {
                validity.push(true)
            }
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
        if let Some(validity) = &mut self.validity {
            validity.push(true)
        }
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
    fn push_empty(&mut self) {
        self.geom_offsets.try_push_usize(0).unwrap();
        if let Some(validity) = &mut self.validity {
            validity.push(true)
        }
    }

    #[inline]
    fn push_null(&mut self) {
        // NOTE! Only the geom_offsets array needs to get extended, because the next geometry will
        // point to the same ring array location
        self.geom_offsets.extend_constant(1);
        match &mut self.validity {
            Some(validity) => validity.push(false),
            None => self.init_validity(),
        }
    }

    #[inline]
    fn init_validity(&mut self) {
        let len = self.geom_offsets.len_proxy();

        let mut validity = MutableBitmap::with_capacity(self.geom_offsets.capacity());
        validity.extend_constant(len, true);
        validity.set(len - 1, false);
        self.validity = Some(validity)
    }
}

impl<O: Offset> Default for MutablePolygonArray<O> {
    fn default() -> Self {
        Self::new()
    }
}

impl<O: Offset> From<MutablePolygonArray<O>> for PolygonArray<O> {
    fn from(other: MutablePolygonArray<O>) -> Self {
        let validity = other.validity.and_then(|x| {
            let bitmap: Bitmap = x.into();
            if bitmap.unset_bits() == 0 {
                None
            } else {
                Some(bitmap)
            }
        });

        let geom_offsets: OffsetsBuffer<O> = other.geom_offsets.into();
        let ring_offsets: OffsetsBuffer<O> = other.ring_offsets.into();

        Self::new(other.coords.into(), geom_offsets, ring_offsets, validity)
    }
}

fn first_pass<'a>(
    geoms: impl Iterator<Item = Option<impl PolygonTrait<'a> + 'a>>,
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

fn second_pass<'a, O: Offset>(
    geoms: impl Iterator<Item = Option<impl PolygonTrait<'a, T = f64> + 'a>>,
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

impl<O: Offset> From<Vec<geo::Polygon>> for MutablePolygonArray<O> {
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

impl<O: Offset> From<Vec<Option<geo::Polygon>>> for MutablePolygonArray<O> {
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

impl<O: Offset> From<bumpalo::collections::Vec<'_, geo::Polygon>> for MutablePolygonArray<O> {
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
impl<O: Offset> From<bumpalo::collections::Vec<'_, Option<geo::Polygon>>>
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

impl<O: Offset> TryFrom<WKBArray<O>> for MutablePolygonArray<O> {
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
impl<O: Offset> From<MutablePolygonArray<O>> for MutableMultiLineStringArray<O> {
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
