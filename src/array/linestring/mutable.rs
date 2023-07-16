use crate::array::{
    LineStringArray, MutableCoordBuffer, MutableInterleavedCoordBuffer, MutableMultiPointArray,
    WKBArray,
};
use crate::error::GeoArrowError;
use crate::geo_traits::LineStringTrait;
use crate::io::native::wkb::linestring::WKBLineString;
use crate::scalar::WKB;
use crate::GeometryArrayTrait;
use arrow2::array::ListArray;
use arrow2::bitmap::{Bitmap, MutableBitmap};
use arrow2::offset::Offsets;
use arrow2::types::Offset;
use geo::CoordsIter;
use std::convert::From;

/// The Arrow equivalent to `Vec<Option<LineString>>`.
/// Converting a [`MutableLineStringArray`] into a [`LineStringArray`] is `O(1)`.
#[derive(Debug, Clone)]
pub struct MutableLineStringArray<O: Offset> {
    coords: MutableCoordBuffer,

    /// Offsets into the coordinate array where each geometry starts
    geom_offsets: Offsets<O>,

    /// Validity is only defined at the geometry level
    validity: Option<MutableBitmap>,
}

impl<O: Offset> MutableLineStringArray<O> {
    /// Creates a new empty [`MutableLineStringArray`].
    pub fn new() -> Self {
        Self::with_capacities(0, 0)
    }

    /// Creates a new [`MutableLineStringArray`] with a capacity.
    pub fn with_capacities(coord_capacity: usize, geom_capacity: usize) -> Self {
        let coords = MutableInterleavedCoordBuffer::with_capacity(coord_capacity);
        Self {
            coords: MutableCoordBuffer::Interleaved(coords),
            geom_offsets: Offsets::<O>::with_capacity(geom_capacity),
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
        validity: Option<MutableBitmap>,
    ) -> Result<Self, GeoArrowError> {
        // Can't pass Offsets into the check, expected OffsetsBuffer
        // use crate::scalar::LineString::array::check;
        // check(&x, &y, validity.as_ref().map(|x| x.len()), &geom_offsets)?;
        Ok(Self {
            coords,
            geom_offsets,
            validity,
        })
    }

    /// Extract the low-level APIs from the [`MutableLineStringArray`].
    pub fn into_inner(self) -> (MutableCoordBuffer, Offsets<O>, Option<MutableBitmap>) {
        (self.coords, self.geom_offsets, self.validity)
    }

    /// Adds a new value to the array.
    pub fn try_push_geo(&mut self, value: Option<geo::LineString>) -> Result<(), GeoArrowError> {
        if let Some(line_string) = value {
            line_string
                .coords_iter()
                .for_each(|c| self.coords.push_coord(c));
            self.try_push_valid()?;
        } else {
            self.push_null();
        }
        Ok(())
    }

    #[inline]
    /// Needs to be called when a valid value was extended to this array.
    /// This is a relatively low level function, prefer `try_push` when you can.
    pub fn try_push_valid(&mut self) -> Result<(), GeoArrowError> {
        let total_length = self.coords.len();
        let offset = self.geom_offsets.last().to_usize();
        let length = total_length
            .checked_sub(offset)
            .ok_or(GeoArrowError::Overflow)?;

        // TODO: remove unwrap
        self.geom_offsets.try_push_usize(length).unwrap();
        if let Some(validity) = &mut self.validity {
            validity.push(true)
        }
        Ok(())
    }

    #[inline]
    fn push_null(&mut self) {
        self.geom_offsets.extend_constant(1);
        match &mut self.validity {
            Some(validity) => validity.push(false),
            None => self.init_validity(),
        }
    }

    fn init_validity(&mut self) {
        let len = self.geom_offsets.len_proxy();

        let mut validity = MutableBitmap::with_capacity(self.geom_offsets.capacity());
        validity.extend_constant(len, true);
        validity.set(len - 1, false);
        self.validity = Some(validity)
    }

    pub fn into_arrow(self) -> ListArray<O> {
        let linestring_arr: LineStringArray<O> = self.into();
        linestring_arr.into_arrow()
    }
}

impl<O: Offset> Default for MutableLineStringArray<O> {
    fn default() -> Self {
        Self::new()
    }
}

impl<O: Offset> From<MutableLineStringArray<O>> for LineStringArray<O> {
    fn from(other: MutableLineStringArray<O>) -> Self {
        let validity = other.validity.and_then(|x| {
            let bitmap: Bitmap = x.into();
            if bitmap.unset_bits() == 0 {
                None
            } else {
                Some(bitmap)
            }
        });

        Self::new(other.coords.into(), other.geom_offsets.into(), validity)
    }
}

impl<O: Offset> From<MutableLineStringArray<O>> for ListArray<O> {
    fn from(arr: MutableLineStringArray<O>) -> Self {
        arr.into_arrow()
    }
}

fn first_pass<'a, O: Offset>(
    geoms: impl Iterator<Item = Option<impl LineStringTrait<'a> + 'a>>,
    geoms_length: usize,
) -> (Offsets<O>, Option<MutableBitmap>) {
    let mut geom_offsets = Offsets::<O>::with_capacity(geoms_length);
    let mut validity = MutableBitmap::with_capacity(geoms_length);

    for maybe_geom in geoms {
        validity.push(maybe_geom.is_some());
        geom_offsets
            .try_push_usize(maybe_geom.as_ref().map_or(0, |geom| geom.num_coords()))
            .unwrap();
    }

    (geom_offsets, Some(validity))
}

fn second_pass<'a, O: Offset>(
    geoms: impl Iterator<Item = Option<impl LineStringTrait<'a, T = f64> + 'a>>,
    geom_offsets: Offsets<O>,
    validity: Option<MutableBitmap>,
) -> MutableLineStringArray<O> {
    let mut coord_buffer =
        MutableInterleavedCoordBuffer::with_capacity(geom_offsets.last().to_usize());

    for geom in geoms.into_iter().flatten() {
        for i in 0..geom.num_coords() {
            coord_buffer.push_coord(geom.coord(i).unwrap())
        }
    }

    MutableLineStringArray {
        coords: MutableCoordBuffer::Interleaved(coord_buffer),
        geom_offsets,
        validity,
    }
}

impl<O: Offset> From<Vec<geo::LineString>> for MutableLineStringArray<O> {
    fn from(geoms: Vec<geo::LineString>) -> Self {
        let (geom_offsets, validity) = first_pass::<O>(geoms.iter().map(Some), geoms.len());
        second_pass(geoms.into_iter().map(Some), geom_offsets, validity)
    }
}

impl<O: Offset> From<Vec<Option<geo::LineString>>> for MutableLineStringArray<O> {
    fn from(geoms: Vec<Option<geo::LineString>>) -> Self {
        let (geom_offsets, validity) =
            first_pass::<O>(geoms.iter().map(|x| x.as_ref()), geoms.len());
        second_pass(geoms.into_iter(), geom_offsets, validity)
    }
}

impl<O: Offset> From<bumpalo::collections::Vec<'_, geo::LineString>> for MutableLineStringArray<O> {
    fn from(geoms: bumpalo::collections::Vec<'_, geo::LineString>) -> Self {
        let (geom_offsets, validity) = first_pass::<O>(geoms.iter().map(Some), geoms.len());
        second_pass(geoms.into_iter().map(Some), geom_offsets, validity)
    }
}

impl<O: Offset> From<bumpalo::collections::Vec<'_, Option<geo::LineString>>>
    for MutableLineStringArray<O>
{
    fn from(geoms: bumpalo::collections::Vec<'_, Option<geo::LineString>>) -> Self {
        let (geom_offsets, validity) =
            first_pass::<O>(geoms.iter().map(|x| x.as_ref()), geoms.len());
        second_pass(geoms.into_iter(), geom_offsets, validity)
    }
}

impl<O: Offset> TryFrom<WKBArray<O>> for MutableLineStringArray<O> {
    type Error = GeoArrowError;

    fn try_from(value: WKBArray<O>) -> Result<Self, Self::Error> {
        let wkb_objects: Vec<Option<WKB<'_, O>>> = value.iter().collect();
        let wkb_objects2: Vec<Option<WKBLineString>> = wkb_objects
            .iter()
            .map(|maybe_wkb| {
                maybe_wkb
                    .as_ref()
                    .map(|wkb| wkb.to_wkb_object().to_line_string())
            })
            .collect();
        let (geom_offsets, validity) =
            first_pass::<O>(wkb_objects2.iter().map(|item| item.as_ref()), value.len());
        Ok(second_pass(
            wkb_objects2.iter().map(|item| item.as_ref()),
            geom_offsets,
            validity,
        ))
    }
}

/// LineString and MultiPoint have the same layout, so enable conversions between the two to change
/// the semantic type
impl<O: Offset> From<MutableLineStringArray<O>> for MutableMultiPointArray<O> {
    fn from(value: MutableLineStringArray<O>) -> Self {
        Self::try_new(value.coords, value.geom_offsets, value.validity).unwrap()
    }
}
