use crate::array::{
    MutableCoordBuffer, MutableInterleavedCoordBuffer, MutableMultiLineStringArray, PolygonArray,
    WKBArray,
};
use crate::error::GeoArrowError;
use crate::geo_traits::{LineStringTrait, PolygonTrait};
use crate::io::native::wkb::polygon::WKBPolygon;
use crate::scalar::WKB;
use crate::trait_::GeometryArrayTrait;
use arrow2::array::ListArray;
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

impl<O: Offset> MutablePolygonArray<O> {
    /// Creates a new empty [`MutableLineStringArray`].
    pub fn new() -> Self {
        Self::with_capacities(0, 0, 0)
    }

    /// Creates a new [`MutableLineStringArray`] with a capacity.
    pub fn with_capacities(
        coord_capacity: usize,
        geom_capacity: usize,
        ring_capacity: usize,
    ) -> Self {
        let coords = MutableInterleavedCoordBuffer::with_capacity(coord_capacity);
        Self {
            coords: MutableCoordBuffer::Interleaved(coords),
            geom_offsets: Offsets::<O>::with_capacity(geom_capacity),
            ring_offsets: Offsets::<O>::with_capacity(ring_capacity),
            validity: None,
        }
    }

    /// The canonical method to create a [`MutableLineStringArray`] out of its internal components.
    /// # Implementation
    /// This function is `O(1)`.
    ///
    /// # Errors
    /// This function errors iff:
    /// * The validity is not `None` and its length is different from `values`'s length
    pub fn try_new(
        coords: MutableCoordBuffer,
        geom_offsets: Offsets<O>,
        ring_offsets: Offsets<O>,
        validity: Option<MutableBitmap>,
    ) -> Result<Self, GeoArrowError> {
        // check(&x, &y, validity.as_ref().map(|x| x.len()))?;
        Ok(Self {
            coords,
            geom_offsets,
            ring_offsets,
            validity,
        })
    }

    /// Extract the low-level APIs from the [`MutableLineStringArray`].
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

fn first_pass<'a, O: Offset>(
    geoms: impl Iterator<Item = Option<impl PolygonTrait<'a> + 'a>>,
    geoms_length: usize,
) -> (Offsets<O>, Offsets<O>, Option<MutableBitmap>) {
    let mut validity = MutableBitmap::with_capacity(geoms_length);

    // Offset into ring indexes for each geometry
    let mut geom_offsets = Offsets::<O>::with_capacity(geoms_length);

    // Offset into coordinates for each ring
    // This capacity will only be enough in the case where each geometry has only a single ring
    let mut ring_offsets = Offsets::<O>::with_capacity(geoms_length);

    // TODO: this is kinda messy; there should be a better way to decide to keep around the
    // validity
    // Also, this will allocate validity even when it doesn't need to be used. Check how arrow2
    // implements this
    let mut invalid_exist = false;

    for maybe_polygon in geoms {
        if let Some(polygon) = maybe_polygon {
            validity.push(true);

            // Total number of rings in this polygon
            let num_interiors = polygon.num_interiors();
            geom_offsets.try_push_usize(num_interiors + 1).unwrap();

            // Number of coords for each ring
            ring_offsets
                .try_push_usize(polygon.exterior().num_coords())
                .unwrap();

            for int_ring_idx in 0..polygon.num_interiors() {
                let int_ring = polygon.interior(int_ring_idx).unwrap();
                ring_offsets.try_push_usize(int_ring.num_coords()).unwrap();
            }
        } else {
            invalid_exist = true;
            validity.push(false);
            geom_offsets.try_push_usize(0).unwrap();
        }
    }

    (
        geom_offsets,
        ring_offsets,
        if invalid_exist { Some(validity) } else { None },
    )
}

fn second_pass<'a, O: Offset>(
    geoms: impl Iterator<Item = Option<impl PolygonTrait<'a, T = f64> + 'a>>,
    geom_offsets: Offsets<O>,
    ring_offsets: Offsets<O>,
    validity: Option<MutableBitmap>,
) -> MutablePolygonArray<O> {
    let mut coord_buffer =
        MutableInterleavedCoordBuffer::with_capacity(ring_offsets.last().to_usize());

    for polygon in geoms.into_iter().flatten() {
        let ext_ring = polygon.exterior();
        for coord_idx in 0..ext_ring.num_coords() {
            let coord = ext_ring.coord(coord_idx).unwrap();
            coord_buffer.push_coord(coord);
        }

        for int_ring_idx in 0..polygon.num_interiors() {
            let int_ring = polygon.interior(int_ring_idx).unwrap();
            for coord_idx in 0..int_ring.num_coords() {
                let coord = int_ring.coord(coord_idx).unwrap();
                coord_buffer.push_coord(coord);
            }
        }
    }

    MutablePolygonArray {
        coords: MutableCoordBuffer::Interleaved(coord_buffer),
        geom_offsets,
        ring_offsets,
        validity,
    }
}

impl<O: Offset> From<Vec<geo::Polygon>> for MutablePolygonArray<O> {
    fn from(geoms: Vec<geo::Polygon>) -> Self {
        let (geom_offsets, ring_offsets, validity) =
            first_pass::<O>(geoms.iter().map(Some), geoms.len());
        second_pass(
            geoms.into_iter().map(Some),
            geom_offsets,
            ring_offsets,
            validity,
        )
    }
}

impl<O: Offset> From<Vec<Option<geo::Polygon>>> for MutablePolygonArray<O> {
    fn from(geoms: Vec<Option<geo::Polygon>>) -> Self {
        let (geom_offsets, ring_offsets, validity) =
            first_pass::<O>(geoms.iter().map(|x| x.as_ref()), geoms.len());
        second_pass(geoms.into_iter(), geom_offsets, ring_offsets, validity)
    }
}

impl<O: Offset> From<bumpalo::collections::Vec<'_, geo::Polygon>> for MutablePolygonArray<O> {
    fn from(geoms: bumpalo::collections::Vec<'_, geo::Polygon>) -> Self {
        let (geom_offsets, ring_offsets, validity) =
            first_pass::<O>(geoms.iter().map(Some), geoms.len());
        second_pass(
            geoms.into_iter().map(Some),
            geom_offsets,
            ring_offsets,
            validity,
        )
    }
}
impl<O: Offset> From<bumpalo::collections::Vec<'_, Option<geo::Polygon>>>
    for MutablePolygonArray<O>
{
    fn from(geoms: bumpalo::collections::Vec<'_, Option<geo::Polygon>>) -> Self {
        let (geom_offsets, ring_offsets, validity) =
            first_pass::<O>(geoms.iter().map(|x| x.as_ref()), geoms.len());
        second_pass(geoms.into_iter(), geom_offsets, ring_offsets, validity)
    }
}

impl<O: Offset> TryFrom<WKBArray<O>> for MutablePolygonArray<O> {
    type Error = GeoArrowError;

    fn try_from(value: WKBArray<O>) -> Result<Self, Self::Error> {
        let wkb_objects: Vec<Option<WKB<'_, O>>> = value.iter().collect();
        let wkb_objects2: Vec<Option<WKBPolygon>> = wkb_objects
            .iter()
            .map(|maybe_wkb| {
                maybe_wkb
                    .as_ref()
                    .map(|wkb| wkb.to_wkb_object().to_polygon())
            })
            .collect();
        let (geom_offsets, ring_offsets, validity) =
            first_pass::<O>(wkb_objects2.iter().map(|item| item.as_ref()), value.len());
        Ok(second_pass(
            wkb_objects2.iter().map(|item| item.as_ref()),
            geom_offsets,
            ring_offsets,
            validity,
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
