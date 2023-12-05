use crate::geo_traits::{
    LineStringTrait, MultiLineStringTrait, MultiPointTrait, MultiPolygonTrait, PointTrait,
    PolygonTrait,
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
use geo::Geometry;
#[cfg(feature = "geozero")]
use geozero::{CoordDimensions, ToWkb};

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
        Self::with_capacity(0)
    }

    /// Initializes a new [`WKBBuilder`] with a pre-allocated capacity of slots.
    pub fn with_capacity(capacity: usize) -> Self {
        Self::with_capacities(capacity, 0)
    }

    /// Initializes a new [`MutableBinaryArray`] with a pre-allocated capacity of slots and values.
    /// # Implementation
    /// This does not allocate the validity.
    pub fn with_capacities(item_capacity: usize, data_capacity: usize) -> Self {
        Self(GenericBinaryBuilder::with_capacity(
            item_capacity,
            data_capacity,
        ))
    }

    /// Push a Point onto the end of this array
    ///
    /// ## Performance
    ///
    /// It is expected to be considerably faster if you convert whole geometry arrays at a time.
    /// E.g. using the `From` implementation from PointArray.
    pub fn push_point(&mut self, geom: impl PointTrait<T = f64>) {
        // TODO: figure out how to write directly to the underlying vec without a copy
        let mut buf = Vec::with_capacity(POINT_WKB_SIZE);
        write_point_as_wkb(&mut buf, &geom).unwrap();
        self.0.append_value(&buf)
    }

    /// Push a LineString onto the end of this array
    ///
    /// ## Performance
    ///
    /// It is expected to be considerably faster if you convert whole geometry arrays at a time.
    /// E.g. using the `From` implementation from LineStringArray.
    pub fn push_line_string(&mut self, geom: impl LineStringTrait<T = f64>) {
        // TODO: figure out how to write directly to the underlying vec without a copy
        let mut buf = Vec::with_capacity(line_string_wkb_size(&geom));
        write_line_string_as_wkb(&mut buf, &geom).unwrap();
        self.0.append_value(&buf)
    }

    /// Push a Polygon onto the end of this array
    ///
    /// ## Performance
    ///
    /// It is expected to be considerably faster if you convert whole geometry arrays at a time.
    /// E.g. using the `From` implementation from PolygonArray.
    pub fn push_polygon(&mut self, geom: impl PolygonTrait<T = f64>) {
        // TODO: figure out how to write directly to the underlying vec without a copy
        let mut buf = Vec::with_capacity(polygon_wkb_size(&geom));
        write_polygon_as_wkb(&mut buf, &geom).unwrap();
        self.0.append_value(&buf)
    }

    /// Push a MultiPoint onto the end of this array
    ///
    /// ## Performance
    ///
    /// It is expected to be considerably faster if you convert whole geometry arrays at a time.
    /// E.g. using the `From` implementation from MultiPointArray.
    pub fn push_multi_point(&mut self, geom: impl MultiPointTrait<T = f64>) {
        // TODO: figure out how to write directly to the underlying vec without a copy
        let mut buf = Vec::with_capacity(multi_point_wkb_size(&geom));
        write_multi_point_as_wkb(&mut buf, &geom).unwrap();
        self.0.append_value(&buf)
    }

    /// Push a MultiLineString onto the end of this array
    ///
    /// ## Performance
    ///
    /// It is expected to be considerably faster if you convert whole geometry arrays at a time.
    /// E.g. using the `From` implementation from MultiLineStringArray.
    pub fn push_multi_line_string(&mut self, geom: impl MultiLineStringTrait<T = f64>) {
        // TODO: figure out how to write directly to the underlying vec without a copy
        let mut buf = Vec::with_capacity(multi_line_string_wkb_size(&geom));
        write_multi_line_string_as_wkb(&mut buf, &geom).unwrap();
        self.0.append_value(&buf)
    }

    /// Push a MultiPolygon onto the end of this array
    ///
    /// ## Performance
    ///
    /// It is expected to be considerably faster if you convert whole geometry arrays at a time.
    /// E.g. using the `From` implementation from MultiPolygonArray.
    pub fn push_multi_polygon(&mut self, geom: impl MultiPolygonTrait<T = f64>) {
        // TODO: figure out how to write directly to the underlying vec without a copy
        let mut buf = Vec::with_capacity(multi_polygon_wkb_size(&geom));
        write_multi_polygon_as_wkb(&mut buf, &geom).unwrap();
        self.0.append_value(&buf)
    }
}

#[cfg(feature = "geozero")]
impl<O: OffsetSizeTrait> From<Vec<Option<Geometry>>> for WKBBuilder<O> {
    fn from(other: Vec<Option<Geometry>>) -> Self {
        let mut wkb_array = GenericBinaryBuilder::with_capacity(other.len(), other.len());

        for geom in other {
            let wkb = geom.map(|g| g.to_wkb(CoordDimensions::xy()).unwrap());
            wkb_array.append_option(wkb);
        }

        Self(wkb_array)
    }
}

#[cfg(not(feature = "geozero"))]
impl<O: OffsetSizeTrait> From<Vec<Option<Geometry>>> for WKBBuilder<O> {
    fn from(_other: Vec<Option<Geometry>>) -> Self {
        panic!("Activate the 'geozero' feature to convert to WKB.")
    }
}

#[cfg(feature = "geozero")]
impl<O: OffsetSizeTrait> From<bumpalo::collections::Vec<'_, Option<Geometry>>> for WKBBuilder<O> {
    fn from(other: bumpalo::collections::Vec<'_, Option<Geometry>>) -> Self {
        let mut wkb_array = GenericBinaryBuilder::with_capacity(other.len(), other.len());

        for geom in other {
            let wkb = geom.map(|g| g.to_wkb(CoordDimensions::xy()).unwrap());
            wkb_array.append_option(wkb);
        }

        Self(wkb_array)
    }
}

#[cfg(not(feature = "geozero"))]
impl<O: OffsetSizeTrait> From<bumpalo::collections::Vec<'_, Option<Geometry>>> for WKBBuilder<O> {
    fn from(_other: bumpalo::collections::Vec<'_, Option<Geometry>>) -> Self {
        panic!("Activate the 'geozero' feature to convert to WKB.")
    }
}

impl<O: OffsetSizeTrait> From<WKBBuilder<O>> for WKBArray<O> {
    fn from(other: WKBBuilder<O>) -> Self {
        Self::new(other.0.finish_cloned())
    }
}
