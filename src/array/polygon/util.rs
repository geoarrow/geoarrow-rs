use std::borrow::Cow;

use arrow_array::OffsetSizeTrait;
use arrow_buffer::OffsetBuffer;

use crate::array::util::OffsetBufferUtils;
use crate::array::CoordBuffer;
use crate::trait_::GeometryArrayAccessor;

pub(crate) fn parse_polygon<O: OffsetSizeTrait>(
    coords: Cow<'_, CoordBuffer>,
    polygon_offsets: Cow<'_, OffsetBuffer<O>>,
    ring_offsets: Cow<'_, OffsetBuffer<O>>,
    i: usize,
) -> geo::Polygon {
    // Start and end indices into the ring_offsets buffer
    let (start_geom_idx, end_geom_idx) = polygon_offsets.start_end(i);

    // Parse exterior ring first
    let (start_ext_ring_idx, end_ext_ring_idx) = ring_offsets.start_end(start_geom_idx);
    let mut exterior_coords: Vec<geo::Coord> =
        Vec::with_capacity(end_ext_ring_idx - start_ext_ring_idx);

    for i in start_ext_ring_idx..end_ext_ring_idx {
        exterior_coords.push(coords.value(i).into());
    }
    let exterior_ring: geo::LineString = exterior_coords.into();

    // Parse any interior rings
    // Note: need to check if interior rings exist otherwise the subtraction below can overflow
    let has_interior_rings = end_geom_idx - start_geom_idx > 1;
    let n_interior_rings = if has_interior_rings {
        end_geom_idx - start_geom_idx - 2
    } else {
        0
    };
    let mut interior_rings: Vec<geo::LineString<f64>> = Vec::with_capacity(n_interior_rings);
    for ring_idx in start_geom_idx + 1..end_geom_idx {
        let (start_coord_idx, end_coord_idx) = ring_offsets.start_end(ring_idx);
        let mut ring: Vec<geo::Coord> = Vec::with_capacity(end_coord_idx - start_coord_idx);
        for coord_idx in start_coord_idx..end_coord_idx {
            ring.push(coords.value(coord_idx).into())
        }
        interior_rings.push(ring.into());
    }

    geo::Polygon::new(exterior_ring, interior_rings)
}
