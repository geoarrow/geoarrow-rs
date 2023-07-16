use super::array::MultiPointArray;
use crate::array::{
    MutableCoordBuffer, MutableInterleavedCoordBuffer, MutableLineStringArray, WKBArray,
};
use crate::error::GeoArrowError;
use crate::geo_traits::{MultiPointTrait, PointTrait};
use crate::io::native::wkb::multipoint::WKBMultiPoint;
use crate::scalar::WKB;
use crate::trait_::{GeometryArrayTrait, MutableGeometryArray};
use arrow2::array::ListArray;
use arrow2::bitmap::{Bitmap, MutableBitmap};
use arrow2::offset::Offsets;
use arrow2::types::Offset;

/// The Arrow equivalent to `Vec<Option<MultiPoint>>`.
/// Converting a [`MutableMultiPointArray`] into a [`MultiPointArray`] is `O(1)`.
#[derive(Debug, Clone)]
pub struct MutableMultiPointArray<O: Offset> {
    coords: MutableCoordBuffer,

    geom_offsets: Offsets<O>,

    /// Validity is only defined at the geometry level
    validity: Option<MutableBitmap>,
}

// Many of the methods here use the From impl from MutableLineStringArray to MutableMultiPointArray
// to DRY

impl<O: Offset> MutableMultiPointArray<O> {
    /// Creates a new empty [`MutableMultiPointArray`].
    pub fn new() -> Self {
        MutableLineStringArray::new().into()
    }

    /// Creates a new [`MutableMultiPointArray`] with a capacity.
    pub fn with_capacities(coord_capacity: usize, geom_capacity: usize) -> Self {
        let coords = MutableInterleavedCoordBuffer::with_capacity(coord_capacity);
        Self {
            coords: MutableCoordBuffer::Interleaved(coords),
            geom_offsets: Offsets::<O>::with_capacity(geom_capacity),
            validity: None,
        }
    }

    /// The canonical method to create a [`MutableMultiPointArray`] out of its internal components.
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
        MutableLineStringArray::try_new(coords, geom_offsets, validity).map(|result| result.into())
    }

    /// Extract the low-level APIs from the [`MutableMultiPointArray`].
    pub fn into_inner(self) -> (MutableCoordBuffer, Offsets<O>, Option<MutableBitmap>) {
        (self.coords, self.geom_offsets, self.validity)
    }

    pub fn into_arrow(self) -> ListArray<O> {
        let arr: MultiPointArray<O> = self.into();
        arr.into_arrow()
    }

    /// Adds a new value to the array.
    pub fn try_push_geo(&mut self, value: Option<geo::MultiPoint>) -> Result<(), GeoArrowError> {
        if let Some(multipoint) = value {
            multipoint
                .0
                .iter()
                .for_each(|point| self.coords.push_coord(point.0));
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
}

impl<O: Offset> Default for MutableMultiPointArray<O> {
    fn default() -> Self {
        Self::new()
    }
}

impl<O: Offset> MutableGeometryArray for MutableMultiPointArray<O> {
    fn len(&self) -> usize {
        self.coords.len()
    }

    fn validity(&self) -> Option<&MutableBitmap> {
        self.validity.as_ref()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

impl<O: Offset> From<MutableMultiPointArray<O>> for MultiPointArray<O> {
    fn from(mut other: MutableMultiPointArray<O>) -> Self {
        let validity = other.validity.and_then(|x| {
            let bitmap: Bitmap = x.into();
            if bitmap.unset_bits() == 0 {
                None
            } else {
                Some(bitmap)
            }
        });

        // TODO: impl shrink_to_fit for all mutable -> * impls
        // other.coords.shrink_to_fit();
        other.geom_offsets.shrink_to_fit();

        Self::new(other.coords.into(), other.geom_offsets.into(), validity)
    }
}

impl<O: Offset> From<MutableMultiPointArray<O>> for ListArray<O> {
    fn from(arr: MutableMultiPointArray<O>) -> Self {
        arr.into_arrow()
    }
}

fn first_pass<'a, O: Offset>(
    geoms: impl Iterator<Item = Option<impl MultiPointTrait<'a> + 'a>>,
    geoms_length: usize,
) -> (Offsets<O>, Option<MutableBitmap>) {
    let mut geom_offsets = Offsets::<O>::with_capacity(geoms_length);
    let mut validity = MutableBitmap::with_capacity(geoms_length);

    for maybe_multi_point in geoms {
        validity.push(maybe_multi_point.is_some());
        geom_offsets
            .try_push_usize(
                maybe_multi_point
                    .as_ref()
                    .map_or(0, |multi_point| multi_point.num_points()),
            )
            .unwrap();
    }

    (geom_offsets, Some(validity))
}

fn second_pass<'a, O: Offset>(
    geoms: impl Iterator<Item = Option<impl MultiPointTrait<'a, T = f64> + 'a>>,
    geom_offsets: Offsets<O>,
    validity: Option<MutableBitmap>,
) -> MutableMultiPointArray<O> {
    let mut coord_buffer =
        MutableInterleavedCoordBuffer::with_capacity(geom_offsets.last().to_usize());

    for multi_point in geoms.into_iter().flatten() {
        for point_idx in 0..multi_point.num_points() {
            let point = multi_point.point(point_idx).unwrap();
            coord_buffer.push_xy(point.x(), point.y());
        }
    }

    MutableMultiPointArray {
        coords: MutableCoordBuffer::Interleaved(coord_buffer),
        geom_offsets,
        validity,
    }
}

impl<O: Offset> From<Vec<geo::MultiPoint>> for MutableMultiPointArray<O> {
    fn from(geoms: Vec<geo::MultiPoint>) -> Self {
        let (geom_offsets, validity) = first_pass::<O>(geoms.iter().map(Some), geoms.len());
        second_pass(geoms.into_iter().map(Some), geom_offsets, validity)
    }
}

impl<O: Offset> From<Vec<Option<geo::MultiPoint>>> for MutableMultiPointArray<O> {
    fn from(geoms: Vec<Option<geo::MultiPoint>>) -> Self {
        let (geom_offsets, validity) =
            first_pass::<O>(geoms.iter().map(|x| x.as_ref()), geoms.len());
        second_pass(geoms.into_iter(), geom_offsets, validity)
    }
}

impl<O: Offset> From<bumpalo::collections::Vec<'_, geo::MultiPoint>> for MutableMultiPointArray<O> {
    fn from(geoms: bumpalo::collections::Vec<'_, geo::MultiPoint>) -> Self {
        let (geom_offsets, validity) = first_pass::<O>(geoms.iter().map(Some), geoms.len());
        second_pass(geoms.into_iter().map(Some), geom_offsets, validity)
    }
}

impl<O: Offset> From<bumpalo::collections::Vec<'_, Option<geo::MultiPoint>>>
    for MutableMultiPointArray<O>
{
    fn from(geoms: bumpalo::collections::Vec<'_, Option<geo::MultiPoint>>) -> Self {
        let (geom_offsets, validity) =
            first_pass::<O>(geoms.iter().map(|x| x.as_ref()), geoms.len());
        second_pass(geoms.into_iter(), geom_offsets, validity)
    }
}

impl<O: Offset> TryFrom<WKBArray<O>> for MutableMultiPointArray<O> {
    type Error = GeoArrowError;

    fn try_from(value: WKBArray<O>) -> Result<Self, Self::Error> {
        let wkb_objects: Vec<Option<WKB<'_, O>>> = value.iter().collect();
        let wkb_objects2: Vec<Option<WKBMultiPoint>> = wkb_objects
            .iter()
            .map(|maybe_wkb| {
                maybe_wkb
                    .as_ref()
                    .map(|wkb| wkb.to_wkb_object().to_multi_point())
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
impl<O: Offset> From<MutableMultiPointArray<O>> for MutableLineStringArray<O> {
    fn from(value: MutableMultiPointArray<O>) -> Self {
        Self::try_new(value.coords, value.geom_offsets, value.validity).unwrap()
    }
}
