use super::array::check;
use crate::array::{MutableCoordBuffer, MutableInterleavedCoordBuffer, PointArray, WKBArray};
use crate::error::GeoArrowError;
use crate::geo_traits::PointTrait;
use crate::io::wkb::reader::point::WKBPoint;
use crate::scalar::WKB;
use crate::trait_::MutableGeometryArray;
use crate::GeometryArrayTrait;
use arrow_array::{Array, OffsetSizeTrait};
use arrow_buffer::{NullBuffer, NullBufferBuilder};
use geo::Point;

/// The Arrow equivalent to `Vec<Option<Point>>`.
/// Converting a [`MutablePointArray`] into a [`PointArray`] is `O(1)`.
#[derive(Debug)]
pub struct MutablePointArray {
    pub coords: MutableCoordBuffer,
    pub validity: NullBufferBuilder,
}

impl MutablePointArray {
    /// Creates a new empty [`MutablePointArray`].
    pub fn new() -> Self {
        Self::with_capacity(0)
    }

    /// Creates a new [`MutablePointArray`] with a capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        let coords = MutableInterleavedCoordBuffer::with_capacity(capacity);
        Self {
            coords: MutableCoordBuffer::Interleaved(coords),
            validity: NullBufferBuilder::new(capacity),
        }
    }

    /// Reserves capacity for at least `additional` more points to be inserted
    /// in the given `Vec<T>`. The collection may reserve more space to
    /// speculatively avoid frequent reallocations. After calling `reserve`,
    /// capacity will be greater than or equal to `self.len() + additional`.
    /// Does nothing if capacity is already sufficient.
    pub fn reserve(&mut self, additional: usize) {
        self.coords.reserve(additional);
        // if let Some(validity) = self.validity.as_mut() {
        //     validity.reserve(additional)
        // }
    }

    /// Reserves the minimum capacity for at least `additional` more points to
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
    pub fn reserve_exact(&mut self, additional: usize) {
        self.coords.reserve_exact(additional);
        // if let Some(validity) = self.validity.as_mut() {
        //     validity.reserve(additional)
        // }
    }

    /// The canonical method to create a [`MutablePointArray`] out of its internal components.
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
    pub fn try_new(
        coords: MutableCoordBuffer,
        validity: NullBufferBuilder,
    ) -> Result<Self, GeoArrowError> {
        // check(&coords.clone().into(), validity.as_ref().map(|x| x.len()))?;
        Ok(Self { coords, validity })
    }

    /// Extract the low-level APIs from the [`MutablePointArray`].
    pub fn into_inner(self) -> (MutableCoordBuffer, NullBufferBuilder) {
        (self.coords, self.validity)
    }

    /// Add a new point to the end of this array.
    #[inline]
    pub fn push_point(&mut self, value: Option<&impl PointTrait<T = f64>>) {
        if let Some(value) = value {
            self.coords.push_xy(value.x(), value.y());
            self.validity.append(true);
        } else {
            self.coords.push_xy(0., 0.);
            self.validity.append(false);
        }
    }

    /// Add a valid but empty point to the end of this array.
    #[inline]
    pub fn push_empty(&mut self) {
        self.coords.push_xy(f64::NAN, f64::NAN);
        self.validity.append(true);
    }

    /// Add a new null value to the end of this array.
    #[inline]
    pub fn push_null(&mut self) {
        self.coords.push_xy(0., 0.);
        self.validity.append(false);
    }
}

impl MutablePointArray {
    pub fn into_arrow(self) -> Box<dyn Array> {
        let point_array: PointArray = self.into();
        point_array.into_arrow()
    }
}

impl MutableGeometryArray for MutablePointArray {
    fn len(&self) -> usize {
        self.coords.len()
    }

    fn validity(&self) -> &NullBufferBuilder {
        &self.validity
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn into_boxed_arrow(self) -> Box<dyn Array> {
        self.into_arrow()
    }
}

impl Default for MutablePointArray {
    fn default() -> Self {
        Self::new()
    }
}

impl From<MutablePointArray> for PointArray {
    fn from(other: MutablePointArray) -> Self {
        let validity = other.validity().finish();
        Self::new(other.coords.into(), validity)
    }
}

impl From<MutablePointArray> for Box<dyn Array> {
    fn from(arr: MutablePointArray) -> Self {
        arr.into_arrow()
    }
}

fn from_coords(
    geoms: impl Iterator<Item = impl PointTrait<T = f64>>,
    geoms_length: usize,
) -> MutablePointArray {
    let mut mutable_array = MutablePointArray::with_capacity(geoms_length);
    geoms
        .into_iter()
        .for_each(|maybe_point| mutable_array.push_point(Some(&maybe_point)));
    mutable_array
}

pub(crate) fn from_nullable_coords(
    geoms: impl Iterator<Item = Option<impl PointTrait<T = f64>>>,
    geoms_length: usize,
) -> MutablePointArray {
    let mut mutable_array = MutablePointArray::with_capacity(geoms_length);
    geoms
        .into_iter()
        .for_each(|maybe_point| mutable_array.push_point(maybe_point.as_ref()));
    mutable_array
}

impl From<Vec<Point>> for MutablePointArray {
    fn from(geoms: Vec<Point>) -> Self {
        let geoms_length = geoms.len();
        from_coords(geoms.into_iter(), geoms_length)
    }
}

impl From<Vec<Option<Point>>> for MutablePointArray {
    fn from(geoms: Vec<Option<Point>>) -> Self {
        let geoms_length = geoms.len();
        from_nullable_coords(geoms.into_iter(), geoms_length)
    }
}

impl From<bumpalo::collections::Vec<'_, Point>> for MutablePointArray {
    fn from(geoms: bumpalo::collections::Vec<'_, Point>) -> Self {
        let geoms_length = geoms.len();
        from_coords(geoms.into_iter(), geoms_length)
    }
}

impl From<bumpalo::collections::Vec<'_, Option<Point>>> for MutablePointArray {
    fn from(geoms: bumpalo::collections::Vec<'_, Option<Point>>) -> Self {
        let geoms_length = geoms.len();
        from_nullable_coords(geoms.into_iter(), geoms_length)
    }
}

impl<O: OffsetSizeTrait> TryFrom<WKBArray<O>> for MutablePointArray {
    type Error = GeoArrowError;

    fn try_from(value: WKBArray<O>) -> Result<Self, Self::Error> {
        todo!()
        // let wkb_objects: Vec<Option<WKB<'_, O>>> = value.iter().collect();
        // let wkb_objects2: Vec<Option<WKBPoint>> = wkb_objects
        //     .iter()
        //     .map(|maybe_wkb| {
        //         maybe_wkb
        //             .as_ref()
        //             .map(|wkb| wkb.to_wkb_object().into_point())
        //     })
        //     .collect();

        // let geoms_length = wkb_objects2.len();
        // Ok(from_nullable_coords(
        //     wkb_objects2.iter().map(|item| item.as_ref()),
        //     geoms_length,
        // ))
    }
}
