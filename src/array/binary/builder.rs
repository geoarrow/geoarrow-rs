use crate::array::binary::WKBCapacity;
use crate::error::{GeoArrowError, Result};
use crate::geo_traits::{
    GeometryTrait, GeometryType, LineStringTrait, MultiLineStringTrait, MultiPointTrait,
    MultiPolygonTrait, PointTrait, PolygonTrait,
};
use crate::io::wkb::writer::linestring::{line_string_wkb_size, write_line_string_as_wkb};
use crate::io::wkb::writer::multilinestring::{
    multi_line_string_wkb_size, write_multi_line_string_as_wkb,
};
use crate::io::wkb::writer::multipoint::{multi_point_wkb_size, write_multi_point_as_wkb};
use crate::io::wkb::writer::multipolygon::{multi_polygon_wkb_size, write_multi_polygon_as_wkb};
use crate::io::wkb::writer::point::{write_point_as_wkb, POINT_WKB_SIZE};
use crate::io::wkb::writer::polygon::{polygon_wkb_size, write_polygon_as_wkb};
use arrow_array::builder::GenericBinaryBuilder;
use arrow_array::OffsetSizeTrait;

use super::array::WKBArray;

/// The Arrow equivalent to `Vec<Option<Geometry>>`.
/// Converting a [`WKBBuilder`] into a [`WKBArray`] is `O(1)`.
#[derive(Debug)]
pub struct WKBBuilder<O: OffsetSizeTrait>(GenericBinaryBuilder<O>);

impl<O: OffsetSizeTrait> Default for WKBBuilder<O> {
    fn default() -> Self {
        Self::new()
    }
}

impl<O: OffsetSizeTrait> WKBBuilder<O> {
    /// Creates a new empty [`WKBBuilder`].
    /// # Implementation
    /// This allocates a [`Vec`] of one element
    pub fn new() -> Self {
        Self::with_capacity(Default::default())
    }

    /// Initializes a new [`WKBBuilder`] with a pre-allocated capacity of slots and values.
    pub fn with_capacity(capacity: WKBCapacity) -> Self {
        Self(GenericBinaryBuilder::with_capacity(
            capacity.offsets_capacity,
            capacity.buffer_capacity,
        ))
    }

    pub fn with_capacity_from_iter<'a>(
        geoms: impl Iterator<Item = Option<&'a (impl GeometryTrait + 'a)>>,
    ) -> Self {
        let counter = WKBCapacity::from_geometries(geoms);
        Self::with_capacity(counter)
    }

    // Upstream APIs don't exist for this yet. To implement this without upstream changes, we could
    // change to using manual `Vec`'s ourselves
    // pub fn reserve(&mut self, capacity: WKBCapacity) {
    // }

    /// Push a Point onto the end of this array
    ///
    /// ## Performance
    ///
    /// It is expected to be considerably faster if you convert whole geometry arrays at a time.
    /// E.g. using the `From` implementation from PointArray.
    pub fn push_point(&mut self, geom: Option<&impl PointTrait<T = f64>>) {
        if let Some(geom) = geom {
            // TODO: figure out how to write directly to the underlying vec without a copy
            let mut buf = Vec::with_capacity(POINT_WKB_SIZE);
            write_point_as_wkb(&mut buf, geom).unwrap();
            self.0.append_value(&buf)
        } else {
            self.0.append_null();
        }
    }

    /// Push a LineString onto the end of this array
    ///
    /// ## Performance
    ///
    /// It is expected to be considerably faster if you convert whole geometry arrays at a time.
    /// E.g. using the `From` implementation from LineStringArray.
    pub fn push_line_string(&mut self, geom: Option<&impl LineStringTrait<T = f64>>) {
        if let Some(geom) = geom {
            // TODO: figure out how to write directly to the underlying vec without a copy
            let mut buf = Vec::with_capacity(line_string_wkb_size(geom));
            write_line_string_as_wkb(&mut buf, geom).unwrap();
            self.0.append_value(&buf)
        } else {
            self.0.append_null()
        }
    }

    /// Push a Polygon onto the end of this array
    ///
    /// ## Performance
    ///
    /// It is expected to be considerably faster if you convert whole geometry arrays at a time.
    /// E.g. using the `From` implementation from PolygonArray.
    pub fn push_polygon(&mut self, geom: Option<&impl PolygonTrait<T = f64>>) {
        if let Some(geom) = geom {
            // TODO: figure out how to write directly to the underlying vec without a copy
            let mut buf = Vec::with_capacity(polygon_wkb_size(geom));
            write_polygon_as_wkb(&mut buf, geom).unwrap();
            self.0.append_value(&buf)
        } else {
            self.0.append_null()
        }
    }

    /// Push a MultiPoint onto the end of this array
    ///
    /// ## Performance
    ///
    /// It is expected to be considerably faster if you convert whole geometry arrays at a time.
    /// E.g. using the `From` implementation from MultiPointArray.
    pub fn push_multi_point(&mut self, geom: Option<&impl MultiPointTrait<T = f64>>) {
        if let Some(geom) = geom {
            // TODO: figure out how to write directly to the underlying vec without a copy
            let mut buf = Vec::with_capacity(multi_point_wkb_size(geom));
            write_multi_point_as_wkb(&mut buf, geom).unwrap();
            self.0.append_value(&buf)
        } else {
            self.0.append_null()
        }
    }

    /// Push a MultiLineString onto the end of this array
    ///
    /// ## Performance
    ///
    /// It is expected to be considerably faster if you convert whole geometry arrays at a time.
    /// E.g. using the `From` implementation from MultiLineStringArray.
    pub fn push_multi_line_string(&mut self, geom: Option<&impl MultiLineStringTrait<T = f64>>) {
        if let Some(geom) = geom {
            // TODO: figure out how to write directly to the underlying vec without a copy
            let mut buf = Vec::with_capacity(multi_line_string_wkb_size(geom));
            write_multi_line_string_as_wkb(&mut buf, geom).unwrap();
            self.0.append_value(&buf)
        } else {
            self.0.append_null()
        }
    }

    /// Push a MultiPolygon onto the end of this array
    ///
    /// ## Performance
    ///
    /// It is expected to be considerably faster if you convert whole geometry arrays at a time.
    /// E.g. using the `From` implementation from MultiPolygonArray.
    pub fn push_multi_polygon(&mut self, geom: Option<&impl MultiPolygonTrait<T = f64>>) {
        if let Some(geom) = geom {
            // TODO: figure out how to write directly to the underlying vec without a copy
            let mut buf = Vec::with_capacity(multi_polygon_wkb_size(geom));
            write_multi_polygon_as_wkb(&mut buf, geom).unwrap();
            self.0.append_value(&buf)
        } else {
            self.0.append_null()
        }
    }

    /// Push a Geometry onto the end of this array
    pub fn push_geometry(&mut self, geom: Option<&impl GeometryTrait<T = f64>>) {
        if let Some(geom) = geom {
            match geom.as_type() {
                GeometryType::Point(point) => self.push_point(Some(point)),
                GeometryType::LineString(line_string) => self.push_line_string(Some(line_string)),
                GeometryType::Polygon(polygon) => self.push_polygon(Some(polygon)),
                GeometryType::MultiPoint(multi_point) => self.push_multi_point(Some(multi_point)),
                GeometryType::MultiLineString(multi_line_string) => {
                    self.push_multi_line_string(Some(multi_line_string))
                }
                GeometryType::MultiPolygon(multi_polygon) => {
                    self.push_multi_polygon(Some(multi_polygon))
                }
                _ => unimplemented!(),
            }
        } else {
            self.0.append_null()
        }
    }

    pub fn extend_from_iter<'a>(
        &mut self,
        geoms: impl Iterator<Item = Option<&'a (impl GeometryTrait<T = f64> + 'a)>>,
    ) {
        geoms
            .into_iter()
            .for_each(|maybe_geom| self.push_geometry(maybe_geom));
    }

    pub fn from_geometries(geoms: &[impl GeometryTrait<T = f64>]) -> Self {
        let mut array = Self::with_capacity_from_iter(geoms.iter().map(Some));
        array.extend_from_iter(geoms.iter().map(Some));
        array
    }

    pub fn from_nullable_geometries(geoms: &[Option<impl GeometryTrait<T = f64>>]) -> Self {
        let mut array = Self::with_capacity_from_iter(geoms.iter().map(|x| x.as_ref()));
        array.extend_from_iter(geoms.iter().map(|x| x.as_ref()));
        array
    }

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

impl<O: OffsetSizeTrait, G: GeometryTrait<T = f64>> TryFrom<&[Option<G>]> for WKBBuilder<O> {
    type Error = GeoArrowError;

    fn try_from(geoms: &[Option<G>]) -> Result<Self> {
        Ok(Self::from_nullable_geometries(geoms))
    }
}

impl<O: OffsetSizeTrait> From<WKBBuilder<O>> for WKBArray<O> {
    fn from(other: WKBBuilder<O>) -> Self {
        Self::new(other.0.finish_cloned())
    }
}
