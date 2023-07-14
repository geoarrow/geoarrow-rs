use crate::trait_::MutableGeometryArray;
use arrow2::array::{MutableArray, MutableBinaryArray};
use arrow2::bitmap::MutableBitmap;
use arrow2::types::Offset;
use geo::Geometry;
#[cfg(feature = "geozero")]
use geozero::{CoordDimensions, ToWkb};

use super::array::WKBArray;

/// The Arrow equivalent to `Vec<Option<Geometry>>`.
/// Converting a [`MutableWKBArray`] into a [`WKBArray`] is `O(1)`.
#[derive(Debug, Clone)]
pub struct MutableWKBArray<O: Offset>(MutableBinaryArray<O>);

impl<O: Offset> Default for MutableWKBArray<O> {
    fn default() -> Self {
        Self::new()
    }
}

impl<O: Offset> MutableWKBArray<O> {
    /// Creates a new empty [`MutableWKBArray`].
    /// # Implementation
    /// This allocates a [`Vec`] of one element
    pub fn new() -> Self {
        Self::with_capacity(0)
    }

    /// Initializes a new [`MutableWKBArray`] with a pre-allocated capacity of slots.
    pub fn with_capacity(capacity: usize) -> Self {
        Self::with_capacities(capacity, 0)
    }

    /// Initializes a new [`MutableBinaryArray`] with a pre-allocated capacity of slots and values.
    /// # Implementation
    /// This does not allocate the validity.
    pub fn with_capacities(capacity: usize, values: usize) -> Self {
        Self(MutableBinaryArray::<i64>::with_capacities(capacity, values))
    }
}

impl<O: Offset> MutableGeometryArray for MutableWKBArray<O> {
    fn len(&self) -> usize {
        self.0.values().len()
    }

    fn validity(&self) -> Option<&MutableBitmap> {
        self.0.validity()
    }

    // fn as_box(&mut self) -> Box<dyn GeometryArray> {
    //     let array: WKBArray = std::mem::take(self).into();
    //     array.boxed()
    // }

    // fn as_arc(&mut self) -> Arc<dyn GeometryArray> {
    //     let array: WKBArray = std::mem::take(self).into();
    //     array.arced()
    // }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

#[cfg(feature = "geozero")]
impl<O: Offset> From<Vec<Option<Geometry>>> for MutableWKBArray<O> {
    fn from(other: Vec<Option<Geometry>>) -> Self {
        let mut wkb_array = MutableBinaryArray::<O>::with_capacity(other.len());

        for geom in other {
            let wkb = geom.map(|g| g.to_wkb(CoordDimensions::xy()).unwrap());
            wkb_array.push(wkb);
        }

        Self(wkb_array)
    }
}

#[cfg(not(feature = "geozero"))]
impl<O: Offset> From<Vec<Option<Geometry>>> for MutableWKBArray<O> {
    fn from(_other: Vec<Option<Geometry>>) -> Self {
        panic!("Activate the 'geozero' feature to convert to WKB.")
    }
}
impl<O: Offset> From<MutableWKBArray<O>> for WKBArray<O> {
    fn from(other: MutableWKBArray<O>) -> Self {
        Self::new(other.0.into())
    }
}
