use std::fmt;

use arrow_array::OffsetSizeTrait;
use geo::MapCoordsInPlace;
use geozero::ToWkt;

use crate::scalar::*;
use crate::trait_::NativeScalar;

/// Write geometry to display formatter
/// This takes inspiration from Shapely, which prints a max of 80 characters for the geometry:
/// https://github.com/shapely/shapely/blob/c3ddf310f108a7f589d763d613d755ac12ab5d4f/shapely/geometry/base.py#L163-L177
pub(crate) fn write_geometry(
    f: &mut fmt::Formatter<'_>,
    mut geom: geo::Geometry,
    max_chars: usize,
) -> fmt::Result {
    geom.map_coords_in_place(|geo::Coord { x, y }| geo::Coord {
        x: (x * 1000.0).trunc() / 1000.0,
        y: (y * 1000.0).trunc() / 1000.0,
    });

    let wkt = geom.to_wkt().unwrap();

    // subtract start and end brackets
    let max_chars = max_chars - 2;
    write!(f, "<")?;
    if wkt.len() > max_chars {
        // Subtract 3 for ...
        let trimmed_wkt = wkt.chars().take(max_chars - 3).collect::<String>();
        f.write_str(trimmed_wkt.as_str())?;
        write!(f, "...")?;
    } else {
        f.write_str(wkt.as_str())?;
    }
    write!(f, ">")?;
    Ok(())
}

impl fmt::Display for Point<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_geometry(f, self.to_geo_geometry(), 80)
    }
}

impl fmt::Display for Rect<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_geometry(f, self.to_geo_geometry(), 80)
    }
}

macro_rules! impl_fmt {
    ($struct_name:ty) => {
        impl fmt::Display for $struct_name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write_geometry(f, self.to_geo_geometry(), 80)
            }
        }
    };
}

impl_fmt!(LineString<'_>);
impl_fmt!(Polygon<'_>);
impl_fmt!(MultiPoint<'_>);
impl_fmt!(MultiLineString<'_>);
impl_fmt!(MultiPolygon<'_>);
impl_fmt!(GeometryCollection<'_>);

impl fmt::Display for Geometry<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_geometry(f, self.to_geo_geometry(), 80)
    }
}

impl<O: OffsetSizeTrait> fmt::Display for WKB<'_, O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<WKB ")?;
        write_geometry(f, self.to_geo_geometry(), 74)?;
        write!(f, ">")?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::NativeArray;
    use crate::array::PointArray;
    use crate::io::wkb::ToWKB;
    use crate::test::{multipolygon, point};
    use crate::trait_::ArrayAccessor;
    use geoarrow_schema::Dimension;

    #[test]
    fn test_display_point() {
        let point_array = point::point_array();
        let result = point_array.value(0).to_string();
        let expected = "<POINT(0 1)>";
        assert_eq!(result, expected);
    }

    #[test]
    fn test_display_point_5_decimals() {
        let point = geo::Point::from((0.12345, 1.23456));
        let point_array: PointArray = (vec![point].as_slice(), Dimension::XY).into();
        let result = point_array.value(0).to_string();
        let expected = "<POINT(0.123 1.234)>";
        assert_eq!(result, expected);
    }

    #[test]
    fn test_display_multipolygon() {
        let multipolygon_array = multipolygon::mp_array();
        let result = multipolygon_array.value(0).to_string();
        let expected =
            "<MULTIPOLYGON(((-111 45,-111 41,-104 41,-104 45,-111 45)),((-111 45,-111 41,...>";
        assert_eq!(result, expected);
    }

    #[test]
    fn test_display_wkb() {
        let array = point::point_array();
        let wkb_array = array.as_ref().to_wkb::<i32>();
        let result = wkb_array.value(0).to_string();
        let expected = "<WKB <POINT(0 1)>>";
        assert_eq!(result, expected);
    }
}
