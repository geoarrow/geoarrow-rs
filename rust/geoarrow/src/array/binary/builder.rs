use std::sync::Arc;

use crate::array::binary::WKBCapacity;
use crate::array::metadata::ArrayMetadata;
use crate::error::{GeoArrowError, Result};
use arrow_array::builder::GenericBinaryBuilder;
use arrow_array::OffsetSizeTrait;
use geo_traits::{
    GeometryCollectionTrait, GeometryTrait, GeometryType, LineStringTrait, MultiLineStringTrait,
    MultiPointTrait, MultiPolygonTrait, PointTrait, PolygonTrait,
};
use wkb::writer::{
    write_geometry_collection, write_line_string, write_multi_line_string, write_multi_point,
    write_multi_polygon, write_point, write_polygon,
};
use wkb::Endianness;

use super::array::WKBArray;

/// The GeoArrow equivalent to `Vec<Option<WKB>>`: a mutable collection of WKB buffers.
///
/// Converting a [`WKBBuilder`] into a [`WKBArray`] is `O(1)`.
#[derive(Debug)]
pub struct WKBBuilder<O: OffsetSizeTrait>(GenericBinaryBuilder<O>, Arc<ArrayMetadata>);

impl<O: OffsetSizeTrait> Default for WKBBuilder<O> {
    fn default() -> Self {
        Self::new()
    }
}

impl<O: OffsetSizeTrait> WKBBuilder<O> {
    /// Creates a new empty [`WKBBuilder`].
    pub fn new() -> Self {
        Self::with_capacity(Default::default())
    }

    /// Creates a new empty [`WKBBuilder`] with the provided options.
    pub fn new_with_options(metadata: Arc<ArrayMetadata>) -> Self {
        Self::with_capacity_and_options(Default::default(), metadata)
    }

    /// Initializes a new [`WKBBuilder`] with a pre-allocated capacity of slots and values.
    pub fn with_capacity(capacity: WKBCapacity) -> Self {
        Self::with_capacity_and_options(capacity, Default::default())
    }

    /// Creates a new empty [`WKBBuilder`] with the provided capacity and options.
    pub fn with_capacity_and_options(capacity: WKBCapacity, metadata: Arc<ArrayMetadata>) -> Self {
        Self(
            GenericBinaryBuilder::with_capacity(
                capacity.offsets_capacity,
                capacity.buffer_capacity,
            ),
            metadata,
        )
    }

    /// Creates a new empty [`WKBBuilder`] with a capacity inferred by the provided geometry
    /// iterator.
    pub fn with_capacity_from_iter<'a>(
        geoms: impl Iterator<Item = Option<&'a (impl GeometryTrait<T = f64> + 'a)>>,
    ) -> Self {
        Self::with_capacity_and_options_from_iter(geoms, Default::default())
    }

    /// Creates a new empty [`WKBBuilder`] with the provided options and a capacity inferred by the
    /// provided geometry iterator.
    pub fn with_capacity_and_options_from_iter<'a>(
        geoms: impl Iterator<Item = Option<&'a (impl GeometryTrait<T = f64> + 'a)>>,
        metadata: Arc<ArrayMetadata>,
    ) -> Self {
        let counter = WKBCapacity::from_geometries(geoms);
        Self::with_capacity_and_options(counter, metadata)
    }

    // Upstream APIs don't exist for this yet. To implement this without upstream changes, we could
    // change to using manual `Vec`'s ourselves
    // pub fn reserve(&mut self, capacity: WKBCapacity) {
    // }

    /// Push a Point onto the end of this builder
    #[inline]
    pub fn push_point(&mut self, geom: Option<&impl PointTrait<T = f64>>) {
        if let Some(geom) = geom {
            write_point(&mut self.0, geom, Endianness::LittleEndian).unwrap();
            self.0.append_value("")
        } else {
            self.0.append_null();
        }
    }

    /// Push a LineString onto the end of this builder
    #[inline]
    pub fn push_line_string(&mut self, geom: Option<&impl LineStringTrait<T = f64>>) {
        if let Some(geom) = geom {
            write_line_string(&mut self.0, geom, Endianness::LittleEndian).unwrap();
            self.0.append_value("")
        } else {
            self.0.append_null()
        }
    }

    /// Push a Polygon onto the end of this builder
    #[inline]
    pub fn push_polygon(&mut self, geom: Option<&impl PolygonTrait<T = f64>>) {
        if let Some(geom) = geom {
            write_polygon(&mut self.0, geom, Endianness::LittleEndian).unwrap();
            self.0.append_value("")
        } else {
            self.0.append_null()
        }
    }

    /// Push a MultiPoint onto the end of this builder
    #[inline]
    pub fn push_multi_point(&mut self, geom: Option<&impl MultiPointTrait<T = f64>>) {
        if let Some(geom) = geom {
            write_multi_point(&mut self.0, geom, Endianness::LittleEndian).unwrap();
            self.0.append_value("")
        } else {
            self.0.append_null()
        }
    }

    /// Push a MultiLineString onto the end of this builder
    #[inline]
    pub fn push_multi_line_string(&mut self, geom: Option<&impl MultiLineStringTrait<T = f64>>) {
        if let Some(geom) = geom {
            write_multi_line_string(&mut self.0, geom, Endianness::LittleEndian).unwrap();
            self.0.append_value("")
        } else {
            self.0.append_null()
        }
    }

    /// Push a MultiPolygon onto the end of this builder
    #[inline]
    pub fn push_multi_polygon(&mut self, geom: Option<&impl MultiPolygonTrait<T = f64>>) {
        if let Some(geom) = geom {
            write_multi_polygon(&mut self.0, geom, Endianness::LittleEndian).unwrap();
            self.0.append_value("")
        } else {
            self.0.append_null()
        }
    }

    /// Push a Geometry onto the end of this builder
    #[inline]
    pub fn push_geometry(&mut self, geom: Option<&impl GeometryTrait<T = f64>>) {
        use GeometryType::*;

        // TODO: call wkb::write_geometry directly
        if let Some(geom) = geom {
            match geom.as_type() {
                Point(point) => self.push_point(Some(point)),
                LineString(line_string) => self.push_line_string(Some(line_string)),
                Polygon(polygon) => self.push_polygon(Some(polygon)),
                MultiPoint(multi_point) => self.push_multi_point(Some(multi_point)),
                MultiLineString(multi_line_string) => {
                    self.push_multi_line_string(Some(multi_line_string))
                }
                MultiPolygon(multi_polygon) => self.push_multi_polygon(Some(multi_polygon)),
                GeometryCollection(geometry_collection) => {
                    self.push_geometry_collection(Some(geometry_collection))
                }
                Rect(_) | Line(_) | Triangle(_) => todo!(),
            }
        } else {
            self.0.append_null()
        }
    }

    /// Push a GeometryCollection onto the end of this builder
    #[inline]
    pub fn push_geometry_collection(
        &mut self,
        geom: Option<&impl GeometryCollectionTrait<T = f64>>,
    ) {
        if let Some(geom) = geom {
            write_geometry_collection(&mut self.0, geom, Endianness::LittleEndian).unwrap();
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

    /// Create this builder from a slice of Geometries.
    pub fn from_geometries(geoms: &[impl GeometryTrait<T = f64>]) -> Self {
        let mut array = Self::with_capacity_from_iter(geoms.iter().map(Some));
        array.extend_from_iter(geoms.iter().map(Some));
        array
    }

    /// Create this builder from a slice of nullable Geometries.
    pub fn from_nullable_geometries(geoms: &[Option<impl GeometryTrait<T = f64>>]) -> Self {
        let mut array = Self::with_capacity_from_iter(geoms.iter().map(|x| x.as_ref()));
        array.extend_from_iter(geoms.iter().map(|x| x.as_ref()));
        array
    }

    /// Consume this builder and convert to a [WKBArray].
    ///
    /// This is `O(1)`.
    pub fn finish(self) -> WKBArray<O> {
        self.into()
    }
}

impl<O: OffsetSizeTrait, G: GeometryTrait<T = f64>> TryFrom<&[G]> for WKBBuilder<O> {
    type Error = GeoArrowError;

    fn try_from(geoms: &[G]) -> Result<Self> {
        Ok(Self::from_geometries(geoms))
    }
}

impl<O: OffsetSizeTrait, G: GeometryTrait<T = f64>> TryFrom<Vec<Option<G>>> for WKBBuilder<O> {
    type Error = GeoArrowError;

    fn try_from(geoms: Vec<Option<G>>) -> Result<Self> {
        Ok(Self::from_nullable_geometries(&geoms))
    }
}

impl<O: OffsetSizeTrait> From<WKBBuilder<O>> for WKBArray<O> {
    fn from(mut other: WKBBuilder<O>) -> Self {
        Self::new(other.0.finish(), other.1)
    }
}
