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

#[derive(Debug, Clone, Copy)]
pub struct WKBCapacity {
    buffer_capacity: usize,
    offsets_capacity: usize,
}

impl WKBCapacity {
    pub fn new(buffer_capacity: usize, offsets_capacity: usize) -> Self {
        Self {
            buffer_capacity,
            offsets_capacity,
        }
    }

    pub fn new_empty() -> Self {
        Self::new(0, 0)
    }

    pub fn is_empty(&self) -> bool {
        self.buffer_capacity == 0 && self.offsets_capacity == 0
    }

    pub fn buffer_capacity(&self) -> usize {
        self.buffer_capacity
    }

    pub fn offsets_capacity(&self) -> usize {
        self.offsets_capacity
    }

    pub fn add_point(&mut self, is_valid: bool) {
        if is_valid {
            self.buffer_capacity += POINT_WKB_SIZE;
        }
        self.offsets_capacity += 1;
    }

    pub fn add_line_string<'a>(&mut self, line_string: Option<&'a (impl LineStringTrait + 'a)>) {
        if let Some(line_string) = line_string {
            self.buffer_capacity += line_string_wkb_size(line_string);
        }
        self.offsets_capacity += 1;
    }

    pub fn add_polygon<'a>(&mut self, polygon: Option<&'a (impl PolygonTrait + 'a)>) {
        if let Some(polygon) = polygon {
            self.buffer_capacity += polygon_wkb_size(polygon);
        }
        self.offsets_capacity += 1;
    }

    pub fn add_multi_point<'a>(&mut self, multi_point: Option<&'a (impl MultiPointTrait + 'a)>) {
        if let Some(multi_point) = multi_point {
            self.buffer_capacity += multi_point_wkb_size(multi_point);
        }
        self.offsets_capacity += 1;
    }

    pub fn add_multi_line_string<'a>(
        &mut self,
        multi_line_string: Option<&'a (impl MultiLineStringTrait + 'a)>,
    ) {
        if let Some(multi_line_string) = multi_line_string {
            self.buffer_capacity += multi_line_string_wkb_size(multi_line_string);
        }
        self.offsets_capacity += 1;
    }

    pub fn add_multi_polygon<'a>(
        &mut self,
        multi_polygon: Option<&'a (impl MultiPolygonTrait + 'a)>,
    ) {
        if let Some(multi_polygon) = multi_polygon {
            self.buffer_capacity += multi_polygon_wkb_size(multi_polygon);
        }
        self.offsets_capacity += 1;
    }

    pub fn add_geometry<'a>(&mut self, geom: Option<&'a (impl GeometryTrait + 'a)>) {
        if let Some(geom) = geom {
            match geom.as_type() {
                crate::geo_traits::GeometryType::Point(_) => self.add_point(true),
                crate::geo_traits::GeometryType::LineString(g) => self.add_line_string(Some(g)),
                crate::geo_traits::GeometryType::Polygon(g) => self.add_polygon(Some(g)),
                crate::geo_traits::GeometryType::MultiPoint(p) => self.add_multi_point(Some(p)),
                crate::geo_traits::GeometryType::MultiLineString(p) => {
                    self.add_multi_line_string(Some(p))
                }
                crate::geo_traits::GeometryType::MultiPolygon(p) => self.add_multi_polygon(Some(p)),
                crate::geo_traits::GeometryType::GeometryCollection(_) => {
                    panic!("nested geometry collections not supported")
                }
                _ => todo!(),
            }
        } else {
            self.offsets_capacity += 1;
        }
    }

    pub fn from_points<'a>(
        geoms: impl Iterator<Item = Option<&'a (impl PointTrait + 'a)>>,
    ) -> Self {
        let mut counter = Self::new_empty();
        for maybe_geom in geoms.into_iter() {
            counter.add_point(maybe_geom.is_some());
        }
        counter
    }

    pub fn from_line_strings<'a>(
        geoms: impl Iterator<Item = Option<&'a (impl LineStringTrait + 'a)>>,
    ) -> Self {
        let mut counter = Self::new_empty();
        for maybe_geom in geoms.into_iter() {
            counter.add_line_string(maybe_geom);
        }
        counter
    }

    pub fn from_polygons<'a>(
        geoms: impl Iterator<Item = Option<&'a (impl PolygonTrait + 'a)>>,
    ) -> Self {
        let mut counter = Self::new_empty();
        for maybe_geom in geoms.into_iter() {
            counter.add_polygon(maybe_geom);
        }
        counter
    }

    pub fn from_multi_points<'a>(
        geoms: impl Iterator<Item = Option<&'a (impl MultiPointTrait + 'a)>>,
    ) -> Self {
        let mut counter = Self::new_empty();
        for maybe_geom in geoms.into_iter() {
            counter.add_multi_point(maybe_geom);
        }
        counter
    }

    pub fn from_multi_line_strings<'a>(
        geoms: impl Iterator<Item = Option<&'a (impl MultiLineStringTrait + 'a)>>,
    ) -> Self {
        let mut counter = Self::new_empty();
        for maybe_geom in geoms.into_iter() {
            counter.add_multi_line_string(maybe_geom);
        }
        counter
    }

    pub fn from_multi_polygons<'a>(
        geoms: impl Iterator<Item = Option<&'a (impl MultiPolygonTrait + 'a)>>,
    ) -> Self {
        let mut counter = Self::new_empty();
        for maybe_geom in geoms.into_iter() {
            counter.add_multi_polygon(maybe_geom);
        }
        counter
    }

    pub fn from_geometries<'a>(
        geoms: impl Iterator<Item = Option<&'a (impl GeometryTrait + 'a)>>,
    ) -> Self {
        let mut counter = Self::new_empty();
        for maybe_geom in geoms.into_iter() {
            counter.add_geometry(maybe_geom);
        }
        counter
    }

    pub fn from_owned_geometries<'a>(
        geoms: impl Iterator<Item = Option<(impl GeometryTrait + 'a)>>,
    ) -> Self {
        let mut counter = Self::new_empty();
        for maybe_geom in geoms.into_iter() {
            counter.add_geometry(maybe_geom.as_ref());
        }
        counter
    }
}

impl Default for WKBCapacity {
    fn default() -> Self {
        Self::new_empty()
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
