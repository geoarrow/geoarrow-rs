use crate::array::{MultiPolygonArray, MutableCoordBuffer, MutableInterleavedCoordBuffer};
use crate::error::GeoArrowError;
use crate::GeometryArrayTrait;
use arrow2::array::ListArray;
use arrow2::bitmap::{Bitmap, MutableBitmap};
use arrow2::offset::{Offsets, OffsetsBuffer};
use arrow2::types::{Index, Offset};
use geo::MultiPolygon;

pub type MutableMultiPolygonParts = (
    MutableCoordBuffer,
    Offsets<i64>,
    Offsets<i64>,
    Offsets<i64>,
    Option<MutableBitmap>,
);

/// The Arrow equivalent to `Vec<Option<MultiPolygon>>`.
/// Converting a [`MutableMultiPolygonArray`] into a [`MultiPolygonArray`] is `O(1)`.
#[derive(Debug, Clone)]
pub struct MutableMultiPolygonArray<O: Offset> {
    coords: MutableCoordBuffer,

    /// Offsets into the ring array where each geometry starts
    geom_offsets: Offsets<O>,

    /// Offsets into the ring array where each polygon starts
    polygon_offsets: Offsets<O>,

    /// Offsets into the coordinate array where each ring starts
    ring_offsets: Offsets<O>,

    /// Validity is only defined at the geometry level
    validity: Option<MutableBitmap>,
}

impl<O: Offset> MutableMultiPolygonArray<O> {
    /// Creates a new empty [`MutableLineStringArray`].
    pub fn new() -> Self {
        Self::with_capacities(0, 0, 0, 0)
    }

    /// Creates a new [`MutableLineStringArray`] with a capacity.
    pub fn with_capacities(
        coord_capacity: usize,
        geom_capacity: usize,
        polygon_capacity: usize,
        ring_capacity: usize,
    ) -> Self {
        let coords = MutableInterleavedCoordBuffer::with_capacity(coord_capacity);
        Self {
            coords: MutableCoordBuffer::Interleaved(coords),
            geom_offsets: Offsets::<O>::with_capacity(geom_capacity),
            polygon_offsets: Offsets::<O>::with_capacity(polygon_capacity),
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
        polygon_offsets: Offsets<O>,
        ring_offsets: Offsets<O>,
        validity: Option<MutableBitmap>,
    ) -> Result<Self, GeoArrowError> {
        // check(&x, &y, validity.as_ref().map(|x| x.len()))?;
        Ok(Self {
            coords,
            geom_offsets,
            polygon_offsets,
            ring_offsets,
            validity,
        })
    }

    /// Extract the low-level APIs from the [`MutableLineStringArray`].
    pub fn into_inner(self) -> MutableMultiPolygonParts {
        (
            self.coords,
            self.geom_offsets,
            self.polygon_offsets,
            self.ring_offsets,
            self.validity,
        )
    }

    pub fn into_arrow(self) -> ListArray<i64> {
        let arr: MultiPolygonArray = self.into();
        arr.into_arrow()
    }
}

impl<O: Offset> Default for MutableMultiPolygonArray<O> {
    fn default() -> Self {
        Self::new()
    }
}

impl<O: Offset> From<MutableMultiPolygonArray<O>> for MultiPolygonArray<O> {
    fn from(other: MutableMultiPolygonArray<O>) -> Self {
        let validity = other.validity.and_then(|x| {
            let bitmap: Bitmap = x.into();
            if bitmap.unset_bits() == 0 {
                None
            } else {
                Some(bitmap)
            }
        });

        let geom_offsets: OffsetsBuffer<O> = other.geom_offsets.into();
        let polygon_offsets: OffsetsBuffer<O> = other.polygon_offsets.into();
        let ring_offsets: OffsetsBuffer<O> = other.ring_offsets.into();

        Self::new(
            other.coords.into(),
            geom_offsets,
            polygon_offsets,
            ring_offsets,
            validity,
        )
    }
}

impl<O: Offset> From<Vec<MultiPolygon>> for MutableMultiPolygonArray<O> {
    fn from(geoms: Vec<MultiPolygon>) -> Self {
        use geo::coords_iter::CoordsIter;

        // Offset into polygon indexes for each geometry
        let mut geom_offsets = Offsets::<O>::with_capacity(geoms.len());

        // Offset into rings for each polygon
        // This capacity will only be enough in the case where each geometry has only a single
        // polygon
        let mut polygon_offsets = Offsets::<O>::with_capacity(geoms.len());

        // Offset into coordinates for each ring
        // This capacity will only be enough in the case where each polygon has only a single ring
        let mut ring_offsets = Offsets::<O>::with_capacity(geoms.len());

        for multipolygon in &geoms {
            // Total number of polygons in this MultiPolygon
            geom_offsets.try_push_usize(multipolygon.0.len()).unwrap();

            for polygon in multipolygon {
                // Total number of rings in this Multipolygon
                polygon_offsets
                    .try_push_usize(polygon.interiors().len() + 1)
                    .unwrap();

                // Number of coords for each ring
                ring_offsets
                    .try_push_usize(polygon.exterior().coords_count())
                    .unwrap();

                for int_ring in polygon.interiors() {
                    ring_offsets
                        .try_push_usize(int_ring.coords_count())
                        .unwrap();
                }
            }
        }

        let mut coord_buffer =
            MutableInterleavedCoordBuffer::with_capacity(ring_offsets.last().to_usize());

        for multipolygon in geoms {
            for polygon in multipolygon {
                let ext_ring = polygon.exterior();
                for coord in ext_ring.coords_iter() {
                    coord_buffer.push_coord(coord);
                }

                for int_ring in polygon.interiors() {
                    for coord in int_ring.coords_iter() {
                        coord_buffer.push_coord(coord);
                    }
                }
            }
        }

        MutableMultiPolygonArray {
            coords: MutableCoordBuffer::Interleaved(coord_buffer),
            geom_offsets,
            polygon_offsets,
            ring_offsets,
            validity: None,
        }
    }
}

impl<O: Offset> From<Vec<Option<MultiPolygon>>> for MutableMultiPolygonArray<O> {
    fn from(geoms: Vec<Option<MultiPolygon>>) -> Self {
        use geo::coords_iter::CoordsIter;

        let mut validity = MutableBitmap::with_capacity(geoms.len());

        // Offset into polygon indexes for each geometry
        let mut geom_offsets = Offsets::<O>::with_capacity(geoms.len());

        // Offset into rings for each polygon
        // This capacity will only be enough in the case where each geometry has only a single
        // polygon
        let mut polygon_offsets = Offsets::<O>::with_capacity(geoms.len());

        // Offset into coordinates for each ring
        // This capacity will only be enough in the case where each polygon has only a single ring
        let mut ring_offsets = Offsets::<O>::with_capacity(geoms.len());

        for maybe_multipolygon in &geoms {
            if let Some(multipolygon) = maybe_multipolygon {
                validity.push(true);

                // Total number of polygons in this MultiPolygon
                geom_offsets.try_push_usize(multipolygon.0.len()).unwrap();

                for polygon in multipolygon {
                    // Total number of rings in this Multipolygon
                    polygon_offsets
                        .try_push_usize(polygon.interiors().len() + 1)
                        .unwrap();

                    // Number of coords for each ring
                    ring_offsets
                        .try_push_usize(polygon.exterior().coords_count())
                        .unwrap();

                    for int_ring in polygon.interiors() {
                        ring_offsets
                            .try_push_usize(int_ring.coords_count())
                            .unwrap();
                    }
                }
            } else {
                validity.push(false);
                geom_offsets.try_push_usize(0).unwrap();
            }
        }

        let mut coord_buffer =
            MutableInterleavedCoordBuffer::with_capacity(geom_offsets.last().to_usize());

        for multipolygon in geoms.into_iter().flatten() {
            for polygon in multipolygon {
                let ext_ring = polygon.exterior();
                for coord in ext_ring.coords_iter() {
                    coord_buffer.push_coord(coord);
                }

                for int_ring in polygon.interiors() {
                    for coord in int_ring.coords_iter() {
                        coord_buffer.push_coord(coord);
                    }
                }
            }
        }

        MutableMultiPolygonArray {
            coords: MutableCoordBuffer::Interleaved(coord_buffer),
            geom_offsets,
            polygon_offsets,
            ring_offsets,
            validity: Some(validity),
        }
    }
}
