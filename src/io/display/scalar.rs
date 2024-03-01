use std::fmt;

use arrow_array::OffsetSizeTrait;
use geo::MapCoords;
use geozero::ToWkt;

use crate::io::geo::{
    geometry_collection_to_geo, geometry_to_geo, line_string_to_geo, multi_line_string_to_geo,
    multi_point_to_geo, multi_polygon_to_geo, point_to_geo, polygon_to_geo, rect_to_geo,
};
use crate::scalar::*;

impl fmt::Display for Point<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let geo_geometry = point_to_geo(self);

        // Truncate to max 3 decimal points
        let truncated = geo_geometry.map_coords(|geo::Coord { x, y }| geo::Coord {
            x: (x * 1000.0).round() / 1000.0,
            y: (y * 1000.0).round() / 1000.0,
        });

        write!(f, "<")?;
        f.write_str(geo::Geometry::Point(truncated).to_wkt().unwrap().as_str())?;
        write!(f, ">")?;
        Ok(())
    }
}

impl fmt::Display for Rect<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let geo_geometry = rect_to_geo(self);

        // Truncate to max 3 decimal points
        let truncated = geo_geometry.map_coords(|geo::Coord { x, y }| geo::Coord {
            x: (x * 1000.0).round() / 1000.0,
            y: (y * 1000.0).round() / 1000.0,
        });

        write!(f, "<")?;
        f.write_str(geo::Geometry::Rect(truncated).to_wkt().unwrap().as_str())?;
        write!(f, ">")?;
        Ok(())
    }
}

macro_rules! impl_fmt {
    ($struct_name:ty, $conversion_fn:ident, $geo_geom_type:path) => {
        impl<O: OffsetSizeTrait> fmt::Display for $struct_name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                let geo_geometry = $conversion_fn(self);

                // Truncate to max 3 decimal points
                let truncated = geo_geometry.map_coords(|geo::Coord { x, y }| geo::Coord {
                    x: (x * 1000.0).round() / 1000.0,
                    y: (y * 1000.0).round() / 1000.0,
                });

                write!(f, "<")?;
                f.write_str($geo_geom_type(truncated).to_wkt().unwrap().as_str())?;
                write!(f, ">")?;
                Ok(())
            }
        }
    };
}

impl_fmt!(
    LineString<'_, O>,
    line_string_to_geo,
    geo::Geometry::LineString
);
impl_fmt!(Polygon<'_, O>, polygon_to_geo, geo::Geometry::Polygon);
impl_fmt!(
    MultiPoint<'_, O>,
    multi_point_to_geo,
    geo::Geometry::MultiPoint
);
impl_fmt!(
    MultiLineString<'_, O>,
    multi_line_string_to_geo,
    geo::Geometry::MultiLineString
);
impl_fmt!(
    MultiPolygon<'_, O>,
    multi_polygon_to_geo,
    geo::Geometry::MultiPolygon
);
impl_fmt!(
    GeometryCollection<'_, O>,
    geometry_collection_to_geo,
    geo::Geometry::GeometryCollection
);

impl<O: OffsetSizeTrait> fmt::Display for Geometry<'_, O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let geo_geometry = geometry_to_geo(self);

        // Truncate to max 3 decimal points
        let truncated = geo_geometry.map_coords(|geo::Coord { x, y }| geo::Coord {
            x: (x * 1000.0).round() / 1000.0,
            y: (y * 1000.0).round() / 1000.0,
        });

        write!(f, "<")?;
        f.write_str(truncated.to_wkt().unwrap().as_str())?;
        write!(f, ">")?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::array::PointArray;
    use crate::test::point;
    use crate::trait_::GeometryArrayAccessor;

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
        let point_array: PointArray = vec![point].as_slice().into();
        let result = point_array.value(0).to_string();
        let expected = "<POINT(0.123 1.235)>";
        assert_eq!(result, expected);
    }
}
