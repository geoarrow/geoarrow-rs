use crate::array::{
    MultiLineStringArray, MutableCoordBuffer, MutableInterleavedCoordBuffer, MutablePolygonArray,
    WKBArray,
};
use crate::error::GeoArrowError;
use crate::geo_traits::{CoordTrait, LineStringTrait, MultiLineStringTrait};
use crate::io::native::wkb::maybe_multi_line_string::WKBMaybeMultiLineString;
use crate::scalar::WKB;
use crate::GeometryArrayTrait;
use arrow2::array::ListArray;
use arrow2::bitmap::{Bitmap, MutableBitmap};
use arrow2::offset::{Offsets, OffsetsBuffer};
use arrow2::types::Offset;

#[derive(Debug, Clone)]
pub struct MutableMultiLineStringArray<O: Offset> {
    coords: MutableCoordBuffer,

    /// Offsets into the ring array where each geometry starts
    geom_offsets: Offsets<O>,

    /// Offsets into the coordinate array where each ring starts
    ring_offsets: Offsets<O>,

    /// Validity is only defined at the geometry level
    validity: Option<MutableBitmap>,
}

pub type MultiLineStringInner<O> = (
    MutableCoordBuffer,
    Offsets<O>,
    Offsets<O>,
    Option<MutableBitmap>,
);

impl<'a, O: Offset> MutableMultiLineStringArray<O> {
    /// Creates a new empty [`MutableMultiLineStringArray`].
    pub fn new() -> Self {
        MutablePolygonArray::new().into()
    }

    /// Creates a new [`MutableMultiLineStringArray`] with a capacity.
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

    /// The canonical method to create a [`MutableMultiLineStringArray`] out of its internal components.
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
        MutablePolygonArray::try_new(coords, geom_offsets, ring_offsets, validity)
            .map(|result| result.into())
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

    pub fn into_arrow(self) -> ListArray<O> {
        let arr: MultiLineStringArray<O> = self.into();
        arr.into_arrow()
    }

    /// Add a new LineString to the end of this array.
    ///
    /// # Errors
    ///
    /// This function errors iff the new last item is larger than what O supports.
    pub fn push_line_string(
        &mut self,
        _value: Option<impl LineStringTrait<'a, T = f64>>,
    ) -> Result<(), GeoArrowError> {
        // Push a single line string into this multi line string array
        todo!()
    }

    /// Add a new MultiLineString to the end of this array.
    ///
    /// # Errors
    ///
    /// This function errors iff the new last item is larger than what O supports.
    pub fn push_multi_line_string(
        &mut self,
        value: Option<impl MultiLineStringTrait<'a, T = f64>>,
    ) -> Result<(), GeoArrowError> {
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

impl<O: Offset> Default for MutableMultiLineStringArray<O> {
    fn default() -> Self {
        Self::new()
    }
}

impl<O: Offset> From<MutableMultiLineStringArray<O>> for MultiLineStringArray<O> {
    fn from(other: MutableMultiLineStringArray<O>) -> Self {
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
    geoms: impl Iterator<Item = Option<impl MultiLineStringTrait<'a> + 'a>>,
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

fn second_pass<'a, O: Offset>(
    geoms: impl Iterator<Item = Option<impl MultiLineStringTrait<'a, T = f64> + 'a>>,
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

impl<O: Offset> From<Vec<geo::MultiLineString>> for MutableMultiLineStringArray<O> {
    fn from(geoms: Vec<geo::MultiLineString>) -> Self {
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

impl<O: Offset> From<Vec<Option<geo::MultiLineString>>> for MutableMultiLineStringArray<O> {
    fn from(geoms: Vec<Option<geo::MultiLineString>>) -> Self {
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

impl<O: Offset> From<bumpalo::collections::Vec<'_, geo::MultiLineString>>
    for MutableMultiLineStringArray<O>
{
    fn from(geoms: bumpalo::collections::Vec<'_, geo::MultiLineString>) -> Self {
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

impl<O: Offset> From<bumpalo::collections::Vec<'_, Option<geo::MultiLineString>>>
    for MutableMultiLineStringArray<O>
{
    fn from(geoms: bumpalo::collections::Vec<'_, Option<geo::MultiLineString>>) -> Self {
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

impl<O: Offset> TryFrom<WKBArray<O>> for MutableMultiLineStringArray<O> {
    type Error = GeoArrowError;

    fn try_from(value: WKBArray<O>) -> Result<Self, Self::Error> {
        let wkb_objects: Vec<Option<WKB<'_, O>>> = value.iter().collect();
        let wkb_objects2: Vec<Option<WKBMaybeMultiLineString>> = wkb_objects
            .iter()
            .map(|maybe_wkb| {
                maybe_wkb
                    .as_ref()
                    .map(|wkb| wkb.to_wkb_object().into_maybe_multi_line_string())
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
impl<O: Offset> From<MutableMultiLineStringArray<O>> for MutablePolygonArray<O> {
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
