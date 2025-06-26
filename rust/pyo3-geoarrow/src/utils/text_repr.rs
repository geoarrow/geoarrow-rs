use geoarrow_schema::{CoordType, GeoArrowType};

fn repr_coord_type(coord_type: CoordType) -> &'static str {
    match coord_type {
        CoordType::Interleaved => "interleaved",
        CoordType::Separated => "separated",
    }
}

pub(crate) fn text_repr(typ: &GeoArrowType) -> String {
    use GeoArrowType::*;
    match typ {
        Point(typ) => format!(
            "Point(dimension=\"{}\", coord_type=\"{}\")",
            typ.dimension(),
            repr_coord_type(typ.coord_type())
        ),
        LineString(typ) => format!(
            "LineString(dimension=\"{}\", coord_type=\"{}\")",
            typ.dimension(),
            repr_coord_type(typ.coord_type())
        ),
        Polygon(typ) => format!(
            "Polygon(dimension=\"{}\", coord_type=\"{}\")",
            typ.dimension(),
            repr_coord_type(typ.coord_type())
        ),
        MultiPoint(typ) => format!(
            "MultiPoint(dimension=\"{}\", coord_type=\"{}\")",
            typ.dimension(),
            repr_coord_type(typ.coord_type())
        ),
        MultiLineString(typ) => format!(
            "MultiLineString(dimension=\"{}\", coord_type=\"{}\")",
            typ.dimension(),
            repr_coord_type(typ.coord_type())
        ),
        MultiPolygon(typ) => format!(
            "MultiPolygon(dimension=\"{}\", coord_type=\"{}\")",
            typ.dimension(),
            repr_coord_type(typ.coord_type())
        ),
        Geometry(typ) => format!(
            "Geometry(coord_type=\"{}\")",
            repr_coord_type(typ.coord_type())
        ),
        GeometryCollection(typ) => format!(
            "GeometryCollection(dimension=\"{}\", coord_type=\"{}\")",
            typ.dimension(),
            repr_coord_type(typ.coord_type())
        ),
        Rect(typ) => format!("Box(dimension=\"{}\")", typ.dimension()),
        Wkb(_) => "Wkb".to_string(),
        LargeWkb(_) => "LargeWkb".to_string(),
        WkbView(_) => "WkbView".to_string(),
        Wkt(_) => "Wkt".to_string(),
        LargeWkt(_) => "LargeWkt".to_string(),
        WktView(_) => "WktView".to_string(),
    }
}
