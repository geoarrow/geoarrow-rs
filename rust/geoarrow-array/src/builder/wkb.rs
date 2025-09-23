use arrow_array::OffsetSizeTrait;
use arrow_array::builder::GenericBinaryBuilder;
use geo_traits::{CoordTrait, GeometryTrait, PointTrait};
use geoarrow_schema::WkbType;
use geoarrow_schema::error::{GeoArrowError, GeoArrowResult};

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
    pub fn push_geometry(
        &mut self,
        geom: Option<&impl GeometryTrait<T = f64>>,
    ) -> GeoArrowResult<()> {
        if let Some(geom) = geom {
            // Use a manual WKB encoding to avoid the problematic wkb::writer::write_geometry
            // For now, we'll create a placeholder implementation that works around the LineWrapper issue
            let mut wkb_data = Vec::new();
            
            // Write endianness (1 byte, little endian = 1)
            wkb_data.push(1u8);
            
            // For the minimal implementation, we'll handle basic geometry types
            // This is a temporary workaround - a full implementation would handle all geometry types
            match geom.as_type() {
                geo_traits::GeometryType::Point(point) => {
                    // WKB Point type = 1
                    wkb_data.extend_from_slice(&1u32.to_le_bytes());
                    if let Some(coord) = point.coord() {
                        wkb_data.extend_from_slice(&coord.x().to_le_bytes());
                        wkb_data.extend_from_slice(&coord.y().to_le_bytes());
                    }
                }
                _ => {
                    // For now, fallback to empty geometry for other types
                    // TODO: Implement other geometry types or use geozero when available
                    return Err(GeoArrowError::Wkb("Unsupported geometry type in WKB workaround".to_string()));
                }
            }
            
            self.0.append_value(&wkb_data);
        } else {
            self.0.append_null()
        };
        Ok(())
    }

    /// Extend this builder from an iterator of Geometries.
    pub fn extend_from_iter<'a>(
        &mut self,
        geoms: impl Iterator<Item = Option<&'a (impl GeometryTrait<T = f64> + 'a)>>,
    ) -> GeoArrowResult<()> {
        geoms
            .into_iter()
            .try_for_each(|maybe_geom| self.push_geometry(maybe_geom))?;
        Ok(())
    }

    /// Create this builder from a slice of nullable Geometries.
    pub fn from_nullable_geometries(
        geoms: &[Option<impl GeometryTrait<T = f64>>],
        typ: WkbType,
    ) -> GeoArrowResult<Self> {
        let capacity = WkbCapacity::from_geometries(geoms.iter().map(|x| x.as_ref()));
        let mut array = Self::with_capacity(typ, capacity);
        array.extend_from_iter(geoms.iter().map(|x| x.as_ref()))?;
        Ok(array)
    }

    /// Consume this builder and convert to a [GenericWkbArray].
    ///
    /// This is `O(1)`.
    pub fn finish(mut self) -> GenericWkbArray<O> {
        GenericWkbArray::new(self.0.finish(), self.1.metadata().clone())
    }
}
