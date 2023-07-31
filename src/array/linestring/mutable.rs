use super::array::check;
use crate::array::{
    LineStringArray, MutableCoordBuffer, MutableInterleavedCoordBuffer, MutableMultiPointArray,
    WKBArray,
};
use crate::error::{GeoArrowError, Result};
use crate::geo_traits::{CoordTrait, LineStringTrait};
use crate::io::native::wkb::linestring::WKBLineString;
use crate::scalar::WKB;
use crate::GeometryArrayTrait;
use arrow2::array::ListArray;
use arrow2::bitmap::{Bitmap, MutableBitmap};
use arrow2::offset::Offsets;
use arrow2::types::Offset;
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

impl<'a, O: Offset> MutableLineStringArray<O> {
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

    /// Reserves capacity for at least `additional` more LineStrings to be inserted
    /// in the given `Vec<T>`. The collection may reserve more space to
    /// speculatively avoid frequent reallocations. After calling `reserve`,
    /// capacity will be greater than or equal to `self.len() + additional`.
    /// Does nothing if capacity is already sufficient.
    pub fn reserve(&mut self, coord_additional: usize, geom_additional: usize) {
        self.coords.reserve(coord_additional);
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
    pub fn reserve_exact(&mut self, coord_additional: usize, geom_additional: usize) {
        self.coords.reserve_exact(coord_additional);
        self.geom_offsets.reserve(geom_additional);
        if let Some(validity) = self.validity.as_mut() {
            validity.reserve(geom_additional)
        }
    }

    /// The canonical method to create a [`MutableLineStringArray`] out of its internal components.
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
        coords: MutableCoordBuffer,
        geom_offsets: Offsets<O>,
        validity: Option<MutableBitmap>,
    ) -> Result<Self> {
        check(
            &coords.clone().into(),
            validity.as_ref().map(|x| x.len()),
            &geom_offsets.clone().into(),
        )?;
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

    /// Add a new LineString to the end of this array.
    ///
    /// # Errors
    ///
    /// This function errors iff the new last item is larger than what O supports.
    pub fn push_line_string(
        &mut self,
        value: Option<impl LineStringTrait<'a, T = f64>>,
    ) -> Result<()> {
        if let Some(line_string) = value {
            let num_coords = line_string.num_coords();
            for coord_idx in 0..num_coords {
                let coord = line_string.coord(coord_idx).unwrap();
                self.coords.push_xy(coord.x(), coord.y());
            }
            self.try_push_length(num_coords)?;
        } else {
            self.push_null();
        }
        Ok(())
    }

    /// Push a raw coordinate to the underlying coordinate array.
    ///
    /// # Safety
    ///
    /// This is marked as unsafe because care must be taken to ensure that pushing raw coordinates
    /// to the array upholds the necessary invariants of the array.
    pub(crate) unsafe fn push_xy(&mut self, x: f64, y: f64) -> Result<()> {
        self.coords.push_xy(x, y);
        Ok(())
    }

    #[inline]
    fn calculate_added_length(&self) -> Result<usize> {
        let total_length = self.coords.len();
        let offset = self.geom_offsets.last().to_usize();
        total_length
            .checked_sub(offset)
            .ok_or(GeoArrowError::Overflow)
    }

    /// Needs to be called when a valid value was extended to this array.
    /// This is a relatively low level function, prefer `try_push` when you can.
    #[inline]
    pub fn try_push_valid(&mut self) -> Result<()> {
        let length = self.calculate_added_length()?;
        self.try_push_length(length)
    }

    /// Needs to be called when a valid value was extended to this array.
    /// This is a relatively low level function, prefer `try_push` when you can.
    #[inline]
    pub fn try_push_length(&mut self, geom_offsets_length: usize) -> Result<()> {
        self.geom_offsets.try_push_usize(geom_offsets_length)?;
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

fn first_pass<'a>(
    geoms: impl Iterator<Item = Option<impl LineStringTrait<'a> + 'a>>,
    geoms_length: usize,
) -> (usize, usize) {
    let mut coord_capacity = 0;
    let geom_capacity = geoms_length;

    for line_string in geoms.into_iter().flatten() {
        coord_capacity += line_string.num_coords();
    }

    (coord_capacity, geom_capacity)
}

fn second_pass<'a, O: Offset>(
    geoms: impl Iterator<Item = Option<impl LineStringTrait<'a, T = f64> + 'a>>,
    coord_capacity: usize,
    geom_capacity: usize,
) -> MutableLineStringArray<O> {
    let mut array = MutableLineStringArray::with_capacities(coord_capacity, geom_capacity);

    geoms
        .into_iter()
        .try_for_each(|maybe_multi_point| array.push_line_string(maybe_multi_point))
        .unwrap();

    array
}

impl<O: Offset> From<Vec<geo::LineString>> for MutableLineStringArray<O> {
    fn from(geoms: Vec<geo::LineString>) -> Self {
        let (coord_capacity, geom_capacity) = first_pass(geoms.iter().map(Some), geoms.len());
        second_pass(geoms.into_iter().map(Some), coord_capacity, geom_capacity)
    }
}

impl<O: Offset> From<Vec<Option<geo::LineString>>> for MutableLineStringArray<O> {
    fn from(geoms: Vec<Option<geo::LineString>>) -> Self {
        let (coord_capacity, geom_capacity) =
            first_pass(geoms.iter().map(|x| x.as_ref()), geoms.len());
        second_pass(geoms.into_iter(), coord_capacity, geom_capacity)
    }
}

impl<O: Offset> From<bumpalo::collections::Vec<'_, geo::LineString>> for MutableLineStringArray<O> {
    fn from(geoms: bumpalo::collections::Vec<'_, geo::LineString>) -> Self {
        let (coord_capacity, geom_capacity) = first_pass(geoms.iter().map(Some), geoms.len());
        second_pass(geoms.into_iter().map(Some), coord_capacity, geom_capacity)
    }
}

impl<O: Offset> From<bumpalo::collections::Vec<'_, Option<geo::LineString>>>
    for MutableLineStringArray<O>
{
    fn from(geoms: bumpalo::collections::Vec<'_, Option<geo::LineString>>) -> Self {
        let (coord_capacity, geom_capacity) =
            first_pass(geoms.iter().map(|x| x.as_ref()), geoms.len());
        second_pass(geoms.into_iter(), coord_capacity, geom_capacity)
    }
}

impl<O: Offset> TryFrom<WKBArray<O>> for MutableLineStringArray<O> {
    type Error = GeoArrowError;

    fn try_from(value: WKBArray<O>) -> Result<Self> {
        let wkb_objects: Vec<Option<WKB<'_, O>>> = value.iter().collect();
        let wkb_objects2: Vec<Option<WKBLineString>> = wkb_objects
            .iter()
            .map(|maybe_wkb| {
                maybe_wkb
                    .as_ref()
                    .map(|wkb| wkb.to_wkb_object().into_line_string())
            })
            .collect();
        let (coord_capacity, geom_capacity) =
            first_pass(wkb_objects2.iter().map(|item| item.as_ref()), value.len());
        Ok(second_pass(
            wkb_objects2.iter().map(|item| item.as_ref()),
            coord_capacity,
            geom_capacity,
        ))
    }
}

// #[cfg(feature = "geos")]
// impl<O: Offset> TryFrom<Vec<Option<geos::Geometry<'_>>>> for MutableLineStringArray<O> {
//     type Error = GeoArrowError;
//     fn try_from(value: Vec<Option<geos::Geometry>>) -> std::result::Result<Self, Self::Error> {
//         let length = value.len();
//         let geos_linestring_objects: Vec<Option<GEOSLineString>> = value
//             .iter()
//             .map(|geom| {
//                 geom.map(|geom| GEOSLineString::new_unchecked(std::borrow::Cow::Owned(geom)))
//             })
//             .collect();
//         let (coord_capacity, geom_capacity) = first_pass(
//             geos_linestring_objects.iter().map(|item| item.as_ref()),
//             length,
//         );
//         Ok(second_pass(
//             geos_linestring_objects.iter().map(|item| item.as_ref()),
//             coord_capacity,
//             geom_capacity,
//         ))
//     }
// }

/// LineString and MultiPoint have the same layout, so enable conversions between the two to change
/// the semantic type
impl<O: Offset> From<MutableLineStringArray<O>> for MutableMultiPointArray<O> {
    fn from(value: MutableLineStringArray<O>) -> Self {
        Self::try_new(value.coords, value.geom_offsets, value.validity).unwrap()
    }
}
