use arrow_array::OffsetSizeTrait;
use arrow_array::builder::GenericBinaryBuilder;
use geo_traits::GeometryTrait;
use geoarrow_schema::WkbType;
use wkb::Endianness;
use wkb::writer::{WriteOptions, write_geometry};

use crate::array::GenericWkbArray;
use crate::capacity::WkbCapacity;

/// The GeoArrow equivalent to `Vec<Option<Wkb>>`: a mutable collection of Wkb buffers.
///
/// Converting a [`WkbBuilder`] into a [`GenericWkbArray`] is `O(1)`.
#[derive(Debug)]
pub struct WkbBuilder<O: OffsetSizeTrait>(GenericBinaryBuilder<O>, WkbType);

impl<O: OffsetSizeTrait> WkbBuilder<O> {
    /// Creates a new empty [`WkbBuilder`].
    pub fn new(typ: WkbType) -> Self {
        Self::with_capacity(typ, Default::default())
    }

    /// Initializes a new [`WkbBuilder`] with a pre-allocated capacity of slots and values.
    pub fn with_capacity(typ: WkbType, capacity: WkbCapacity) -> Self {
        Self(
            GenericBinaryBuilder::with_capacity(
                capacity.offsets_capacity,
                capacity.buffer_capacity,
            ),
            typ,
        )
    }

    // Upstream APIs don't exist for this yet. To implement this without upstream changes, we could
    // change to using manual `Vec`'s ourselves
    // pub fn reserve(&mut self, capacity: WkbCapacity) {
    // }

    /// Push a Geometry onto the end of this builder
    #[inline]
    pub fn push_geometry(&mut self, geom: Option<&impl GeometryTrait<T = f64>>) {
        if let Some(geom) = geom {
            let wkb_options = WriteOptions {
                endianness: Endianness::LittleEndian,
            };
            write_geometry(&mut self.0, geom, &wkb_options).unwrap();
            self.0.append_value("")
        } else {
            self.0.append_null()
        }
    }

    /// Extend this builder from an iterator of Geometries.
    pub fn extend_from_iter<'a>(
        &mut self,
        geoms: impl Iterator<Item = Option<&'a (impl GeometryTrait<T = f64> + 'a)>>,
    ) {
        geoms
            .into_iter()
            .for_each(|maybe_geom| self.push_geometry(maybe_geom));
    }

    /// Create this builder from a slice of nullable Geometries.
    pub fn from_nullable_geometries(
        geoms: &[Option<impl GeometryTrait<T = f64>>],
        typ: WkbType,
    ) -> Self {
        let capacity = WkbCapacity::from_geometries(geoms.iter().map(|x| x.as_ref()));
        let mut array = Self::with_capacity(typ, capacity);
        array.extend_from_iter(geoms.iter().map(|x| x.as_ref()));
        array
    }

    /// Consume this builder and convert to a [GenericWkbArray].
    ///
    /// This is `O(1)`.
    pub fn finish(mut self) -> GenericWkbArray<O> {
        GenericWkbArray::new(self.0.finish(), self.1.metadata().clone())
    }
}
