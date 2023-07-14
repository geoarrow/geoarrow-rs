use crate::array::{
    MutableCoordBuffer, MutableInterleavedCoordBuffer, MutableMultiLineStringArray, PolygonArray,
};
use crate::error::GeoArrowError;
use crate::trait_::GeometryArrayTrait;
use arrow2::array::ListArray;
use arrow2::bitmap::{Bitmap, MutableBitmap};
use arrow2::offset::{Offsets, OffsetsBuffer};
use arrow2::types::Offset;
use geo::Polygon;

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

impl<O: Offset> From<Vec<Polygon>> for MutablePolygonArray<O> {
    fn from(geoms: Vec<Polygon>) -> Self {
        use geo::coords_iter::CoordsIter;

        // Offset into ring indexes for each geometry
        let mut geom_offsets = Offsets::<O>::with_capacity(geoms.len());

        // Offset into coordinates for each ring
        // This capacity will only be enough in the case where each geometry has only a single ring
        let mut ring_offsets = Offsets::<O>::with_capacity(geoms.len());

        for geom in &geoms {
            // Total number of rings in this polygon
            geom_offsets
                .try_push_usize(geom.interiors().len() + 1)
                .unwrap();

            // Number of coords for each ring
            ring_offsets
                .try_push_usize(geom.exterior().coords_count())
                .unwrap();

            for int_ring in geom.interiors() {
                ring_offsets
                    .try_push_usize(int_ring.coords_count())
                    .unwrap();
            }
        }

        let mut coord_buffer =
            MutableInterleavedCoordBuffer::with_capacity(ring_offsets.last().to_usize());

        for geom in geoms {
            let ext_ring = geom.exterior();
            for coord in ext_ring.coords_iter() {
                coord_buffer.push_coord(coord);
            }

            for int_ring in geom.interiors() {
                for coord in int_ring.coords_iter() {
                    coord_buffer.push_coord(coord);
                }
            }
        }

        MutablePolygonArray {
            coords: MutableCoordBuffer::Interleaved(coord_buffer),
            geom_offsets,
            ring_offsets,
            validity: None,
        }
    }
}

impl<O: Offset> From<Vec<Option<Polygon>>> for MutablePolygonArray<O> {
    fn from(geoms: Vec<Option<Polygon>>) -> Self {
        use geo::coords_iter::CoordsIter;

        let mut validity = MutableBitmap::with_capacity(geoms.len());

        // Offset into ring indexes for each geometry
        let mut geom_offsets = Offsets::<O>::with_capacity(geoms.len());

        // Offset into coordinates for each ring
        // This capacity will only be enough in the case where each geometry has only a single ring
        let mut ring_offsets = Offsets::<O>::with_capacity(geoms.len());

        for geom in &geoms {
            if let Some(geom) = geom {
                validity.push(true);

                // Total number of rings in this polygon
                geom_offsets
                    .try_push_usize(geom.interiors().len() + 1)
                    .unwrap();

                // Number of coords for each ring
                ring_offsets
                    .try_push_usize(geom.exterior().coords_count())
                    .unwrap();

                for int_ring in geom.interiors() {
                    ring_offsets
                        .try_push_usize(int_ring.coords_count())
                        .unwrap();
                }
            } else {
                validity.push(false);
                geom_offsets.try_push_usize(0).unwrap();
            }
        }

        let mut coord_buffer =
            MutableInterleavedCoordBuffer::with_capacity(ring_offsets.last().to_usize());

        for geom in geoms.into_iter().flatten() {
            let ext_ring = geom.exterior();
            for coord in ext_ring.coords_iter() {
                coord_buffer.push_coord(coord);
            }

            for int_ring in geom.interiors() {
                for coord in int_ring.coords_iter() {
                    coord_buffer.push_coord(coord);
                }
            }
        }

        MutablePolygonArray {
            coords: MutableCoordBuffer::Interleaved(coord_buffer),
            geom_offsets,
            ring_offsets,
            validity: Some(validity),
        }
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
