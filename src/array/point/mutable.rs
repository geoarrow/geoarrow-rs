use crate::array::{MutableCoordBuffer, MutableInterleavedCoordBuffer, PointArray, WKBArray};
use crate::error::GeoArrowError;
use crate::geo_traits::PointTrait;
use crate::io::native::wkb::point::WKBPoint;
use crate::scalar::WKB;
use crate::trait_::MutableGeometryArray;
use crate::GeometryArrayTrait;
use arrow2::array::Array;
use arrow2::bitmap::{Bitmap, MutableBitmap};
use arrow2::types::Offset;
use geo::Point;

/// The Arrow equivalent to `Vec<Option<Point>>`.
/// Converting a [`MutablePointArray`] into a [`PointArray`] is `O(1)`.
#[derive(Debug, Clone)]
pub struct MutablePointArray {
    pub coords: MutableCoordBuffer,
    pub validity: Option<MutableBitmap>,
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
            validity: None,
        }
    }

    /// The canonical method to create a [`MutablePointArray`] out of its internal components.
    /// # Implementation
    /// This function is `O(1)`.
    ///
    /// # Errors
    /// This function errors iff:
    /// * The validity is not `None` and its length is different from `values`'s length
    pub fn try_new(
        coords: MutableCoordBuffer,
        validity: Option<MutableBitmap>,
    ) -> Result<Self, GeoArrowError> {
        // check(&x, &y, validity.as_ref().map(|x| x.len()))?;
        Ok(Self { coords, validity })
    }

    /// Extract the low-level APIs from the [`MutablePointArray`].
    pub fn into_inner(self) -> (MutableCoordBuffer, Option<MutableBitmap>) {
        (self.coords, self.validity)
    }

    /// Adds a new value to the array.
    pub fn push_geo(&mut self, _value: Option<Point>) {
        // TODO:
        // have to think more about how to handle validity when pushing to arrays
        // Unlike other geometry types that have a list array at the top level and which allow you
        // to put validity there, when points are in a FixedSizeListArray you need to allocate
        // empty memory for null items.
        todo!()
        // match value {
        //     Some(value) => {
        //         self.coords.push_coord(value.0);
        //         match &mut self.validity {
        //             Some(validity) => validity.push(true),
        //             None => {}
        //         }
        //     }
        //     None => {
        //         self.x.push(f64::default());
        //         self.y.push(f64::default());
        //         match &mut self.validity {
        //             Some(validity) => validity.push(false),
        //             None => {
        //                 self.init_validity();
        //             }
        //         }
        //     }
        // }
    }

    /// Pop a value from the array.
    /// Note if the values is empty, this method will return None.
    pub fn pop_geo(&mut self) -> Option<Point> {
        todo!()
        // let x = self.x.pop()?;
        // let y = self.y.pop()?;
        // let pt = Point::new(x, y);

        // self.validity
        //     .as_mut()
        //     .map(|x| x.pop()?.then_some(pt))
        //     .unwrap_or_else(|| Some(pt))
    }

    fn _init_validity(&mut self) {
        todo!()
        // let mut validity = MutableBitmap::with_capacity(self.x.capacity());
        // validity.extend_constant(self.len(), true);
        // validity.set(self.len() - 1, false);
        // self.validity = Some(validity)
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

impl Default for MutablePointArray {
    fn default() -> Self {
        Self::new()
    }
}

impl From<MutablePointArray> for PointArray {
    fn from(other: MutablePointArray) -> Self {
        let validity = other.validity.and_then(|x| {
            let bitmap: Bitmap = x.into();
            if bitmap.unset_bits() == 0 {
                None
            } else {
                Some(bitmap)
            }
        });

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
    let mut coord_buffer = MutableInterleavedCoordBuffer::with_capacity(geoms_length);

    for point in geoms {
        coord_buffer.push_xy(point.x(), point.y());
    }

    MutablePointArray {
        coords: MutableCoordBuffer::Interleaved(coord_buffer),
        validity: None,
    }
}

fn from_nullable_coords(
    geoms: impl Iterator<Item = Option<impl PointTrait<T = f64>>>,
    geoms_length: usize,
) -> MutablePointArray {
    // TODO:
    // have to think more about how to handle validity when pushing to arrays
    // Unlike other geometry types that have a list array at the top level and which allow you
    // to put validity there, when points are in a FixedSizeListArray you need to allocate
    // empty memory for null items.

    // Note: this is a quick hack to manually deal with the underlying buffer
    // I should come back to better implementations of building up mutable coords
    // let mut coords = vec![0.0_f64; geoms_length * 2];
    let mut coord_buffer = MutableInterleavedCoordBuffer::with_capacity(geoms_length);
    let mut validity = MutableBitmap::with_capacity(geoms_length);

    for maybe_point in geoms {
        if let Some(point) = maybe_point {
            coord_buffer.push_xy(point.x(), point.y());
            validity.push(true);
        } else {
            coord_buffer.push_xy(0.0f64, 0.0f64);
            validity.push(false);
        }
    }

    MutablePointArray {
        coords: MutableCoordBuffer::Interleaved(coord_buffer),
        validity: Some(validity),
    }
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

impl<O: Offset> TryFrom<WKBArray<O>> for MutablePointArray {
    type Error = GeoArrowError;

    fn try_from(value: WKBArray<O>) -> Result<Self, Self::Error> {
        let wkb_objects: Vec<Option<WKB<'_, O>>> = value.iter().collect();
        let wkb_objects2: Vec<Option<WKBPoint>> = wkb_objects
            .iter()
            .map(|maybe_wkb| {
                maybe_wkb
                    .as_ref()
                    .map(|wkb| wkb.to_wkb_object().into_point())
            })
            .collect();

        let geoms_length = wkb_objects2.len();
        Ok(from_nullable_coords(
            wkb_objects2.iter().map(|item| item.as_ref()),
            geoms_length,
        ))
    }
}
