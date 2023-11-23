use crate::algorithm::native::eq::point_eq;
use crate::geo_traits::{CoordTrait, MultiPointTrait, PointTrait};
use crate::io::wkb::reader::coord::WKBCoord;
use crate::io::wkb::reader::geometry::Endianness;
use crate::io::wkb::reader::r#type::WKBOptions;
use std::iter::Cloned;
use std::slice::Iter;

/// A 2D Point in WKB
///
/// See page 66 of <https://portal.ogc.org/files/?artifact_id=25355>.
#[derive(Debug, Clone, Copy)]
pub struct WKBPoint<'a> {
    /// The coordinate inside this WKBPoint
    coord: WKBCoord<'a>,
    options: WKBOptions,
}

impl<'a> WKBPoint<'a> {
    pub fn new(buf: &'a [u8], byte_order: Endianness, offset: u64, options: WKBOptions) -> Self {
        // The space of the byte order + geometry type
        let mut offset = offset + 5;
        // Skip parsing SRID
        if options.has_srid {
            offset += 4;
        }
        let coord = WKBCoord::new(buf, byte_order, offset);
        Self { coord, options }
    }

    /// The number of bytes in this object, including any header
    ///
    /// Note that this is not the same as the length of the underlying buffer
    pub fn size(&self) -> u64 {
        // - 1: byteOrder
        // - 4: wkbType
        // - 4: numPoints
        // - 2 * 8: two f64s
        let mut size = 1 + 4 + (2 * 8);
        if self.options.has_srid {
            size += 4;
        }
        size
    }

    /// Check if this WKBPoint has equal coordinates as some other Point object
    pub fn equals_point(&self, other: impl PointTrait<T = f64>) -> bool {
        // TODO: how is an empty point stored in WKB?
        point_eq(self, other, true)
    }
}

impl<'a> PointTrait for WKBPoint<'a> {
    type T = f64;

    fn x(&self) -> Self::T {
        CoordTrait::x(&self.coord)
    }

    fn y(&self) -> Self::T {
        CoordTrait::y(&self.coord)
    }
}

impl<'a> PointTrait for &WKBPoint<'a> {
    type T = f64;

    fn x(&self) -> Self::T {
        CoordTrait::x(&self.coord)
    }

    fn y(&self) -> Self::T {
        CoordTrait::y(&self.coord)
    }
}

impl<'a> MultiPointTrait for WKBPoint<'a> {
    type T = f64;
    type ItemType<'b> = WKBPoint<'a> where Self: 'b;
    type Iter<'b> = Cloned<Iter<'a, Self::ItemType<'a>>> where Self: 'b;

    fn num_points(&self) -> usize {
        1
    }

    fn point(&self, i: usize) -> Option<Self::ItemType<'_>> {
        if i > self.num_points() {
            return None;
        }

        Some(*self)
    }

    fn points(&self) -> Self::Iter<'_> {
        todo!()
    }
}

impl<'a> MultiPointTrait for &'a WKBPoint<'a> {
    type T = f64;
    type ItemType<'b> = WKBPoint<'a> where Self: 'b;
    type Iter<'b> = Cloned<Iter<'a, Self::ItemType<'a>>> where Self: 'b;

    fn num_points(&self) -> usize {
        1
    }

    fn point(&self, i: usize) -> Option<Self::ItemType<'_>> {
        if i > self.num_points() {
            return None;
        }

        Some(**self)
    }

    fn points(&self) -> Self::Iter<'_> {
        todo!()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::array::{MutableWKBArray, WKBArray};
    use crate::test::point::p0;
    use crate::trait_::GeoArrayAccessor;
    use geozero::{CoordDimensions, ToWkb};

    #[test]
    fn point_round_trip_write_geozero() {
        let point = p0();
        let buf = geo::Geometry::Point(point)
            .to_wkb(CoordDimensions::xy())
            .unwrap();
        let wkb_point = WKBPoint::new(
            &buf,
            Endianness::LittleEndian,
            0,
            WKBOptions {
                is_ewkb: false,
                has_srid: false,
            },
        );

        assert!(wkb_point.equals_point(point));
    }

    #[test]
    fn point_round_trip() {
        let point = p0();
        let mut wkb_array_builder = MutableWKBArray::<i32>::new();
        wkb_array_builder.push_point(point);
        let wkb_array: WKBArray<i32> = wkb_array_builder.into();
        let wkb_point = wkb_array.value(0).to_wkb_object().into_point();

        assert!(wkb_point.equals_point(point));
    }
}
