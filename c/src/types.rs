//! The geoarrow-c type vocabulary.
//!
//! Names and values are copied from geoarrow-c's `geoarrow_type.h` so a
//! consumer of both libraries can use one set of constants. Functions take
//! these as plain `int32_t` rather than by enum type: an out-of-range
//! discriminant is undefined behaviour in Rust but merely a wrong integer in C,
//! so every code is validated on entry.
//!
//! Two families of `GeoArrowType` values have no geoarrow-c counterpart and
//! are defined here following geoarrow-c's own encoding, `(coord_type - 1) *
//! 10000 + (dimensions - 1) * 1000 + geometry_type`: the `GEOMETRYCOLLECTION`
//! values reuse `GeoArrowGeometryType`'s slot 7, and `geoarrow.geometry` (the
//! mixed-type union, which geoarrow-c does not model) takes 991, next to `BOX`
//! at 990.

use std::os::raw::c_char;
use std::sync::Arc;

use geoarrow_schema::{
    BoxType, CoordType, Crs, Dimension, Edges, GeoArrowType as RsType, GeometryCollectionType,
    GeometryType, LineStringType, Metadata, MultiLineStringType, MultiPointType, MultiPolygonType,
    PointType, PolygonType, WkbType, WktType,
};

use crate::error::Error;

#[repr(i32)]
#[allow(non_camel_case_types)]
pub enum GeoArrowDimensions {
    GEOARROW_DIMENSIONS_UNKNOWN = 0,
    GEOARROW_DIMENSIONS_XY = 1,
    GEOARROW_DIMENSIONS_XYZ = 2,
    GEOARROW_DIMENSIONS_XYM = 3,
    GEOARROW_DIMENSIONS_XYZM = 4,
}

#[repr(i32)]
#[allow(non_camel_case_types)]
pub enum GeoArrowCoordType {
    GEOARROW_COORD_TYPE_UNKNOWN = 0,
    GEOARROW_COORD_TYPE_SEPARATE = 1,
    GEOARROW_COORD_TYPE_INTERLEAVED = 2,
}

#[repr(i32)]
#[allow(non_camel_case_types)]
pub enum GeoArrowEdgeType {
    GEOARROW_EDGE_TYPE_PLANAR = 0,
    GEOARROW_EDGE_TYPE_SPHERICAL = 1,
    GEOARROW_EDGE_TYPE_VINCENTY = 2,
    GEOARROW_EDGE_TYPE_THOMAS = 3,
    GEOARROW_EDGE_TYPE_ANDOYER = 4,
    GEOARROW_EDGE_TYPE_KARNEY = 5,
}

#[repr(i32)]
#[allow(non_camel_case_types)]
pub enum GeoArrowType {
    GEOARROW_TYPE_UNINITIALIZED = 0,

    GEOARROW_TYPE_WKB = 100001,
    GEOARROW_TYPE_LARGE_WKB = 100002,
    GEOARROW_TYPE_WKT = 100003,
    GEOARROW_TYPE_LARGE_WKT = 100004,
    GEOARROW_TYPE_WKB_VIEW = 100005,
    GEOARROW_TYPE_WKT_VIEW = 100006,

    GEOARROW_TYPE_BOX = 990,
    GEOARROW_TYPE_BOX_Z = 1990,
    GEOARROW_TYPE_BOX_M = 2990,
    GEOARROW_TYPE_BOX_ZM = 3990,

    GEOARROW_TYPE_GEOMETRY = 991,
    GEOARROW_TYPE_INTERLEAVED_GEOMETRY = 10991,

    GEOARROW_TYPE_POINT = 1,
    GEOARROW_TYPE_LINESTRING = 2,
    GEOARROW_TYPE_POLYGON = 3,
    GEOARROW_TYPE_MULTIPOINT = 4,
    GEOARROW_TYPE_MULTILINESTRING = 5,
    GEOARROW_TYPE_MULTIPOLYGON = 6,
    GEOARROW_TYPE_GEOMETRYCOLLECTION = 7,

    GEOARROW_TYPE_POINT_Z = 1001,
    GEOARROW_TYPE_LINESTRING_Z = 1002,
    GEOARROW_TYPE_POLYGON_Z = 1003,
    GEOARROW_TYPE_MULTIPOINT_Z = 1004,
    GEOARROW_TYPE_MULTILINESTRING_Z = 1005,
    GEOARROW_TYPE_MULTIPOLYGON_Z = 1006,
    GEOARROW_TYPE_GEOMETRYCOLLECTION_Z = 1007,

    GEOARROW_TYPE_POINT_M = 2001,
    GEOARROW_TYPE_LINESTRING_M = 2002,
    GEOARROW_TYPE_POLYGON_M = 2003,
    GEOARROW_TYPE_MULTIPOINT_M = 2004,
    GEOARROW_TYPE_MULTILINESTRING_M = 2005,
    GEOARROW_TYPE_MULTIPOLYGON_M = 2006,
    GEOARROW_TYPE_GEOMETRYCOLLECTION_M = 2007,

    GEOARROW_TYPE_POINT_ZM = 3001,
    GEOARROW_TYPE_LINESTRING_ZM = 3002,
    GEOARROW_TYPE_POLYGON_ZM = 3003,
    GEOARROW_TYPE_MULTIPOINT_ZM = 3004,
    GEOARROW_TYPE_MULTILINESTRING_ZM = 3005,
    GEOARROW_TYPE_MULTIPOLYGON_ZM = 3006,
    GEOARROW_TYPE_GEOMETRYCOLLECTION_ZM = 3007,

    GEOARROW_TYPE_INTERLEAVED_POINT = 10001,
    GEOARROW_TYPE_INTERLEAVED_LINESTRING = 10002,
    GEOARROW_TYPE_INTERLEAVED_POLYGON = 10003,
    GEOARROW_TYPE_INTERLEAVED_MULTIPOINT = 10004,
    GEOARROW_TYPE_INTERLEAVED_MULTILINESTRING = 10005,
    GEOARROW_TYPE_INTERLEAVED_MULTIPOLYGON = 10006,
    GEOARROW_TYPE_INTERLEAVED_GEOMETRYCOLLECTION = 10007,

    GEOARROW_TYPE_INTERLEAVED_POINT_Z = 11001,
    GEOARROW_TYPE_INTERLEAVED_LINESTRING_Z = 11002,
    GEOARROW_TYPE_INTERLEAVED_POLYGON_Z = 11003,
    GEOARROW_TYPE_INTERLEAVED_MULTIPOINT_Z = 11004,
    GEOARROW_TYPE_INTERLEAVED_MULTILINESTRING_Z = 11005,
    GEOARROW_TYPE_INTERLEAVED_MULTIPOLYGON_Z = 11006,
    GEOARROW_TYPE_INTERLEAVED_GEOMETRYCOLLECTION_Z = 11007,

    GEOARROW_TYPE_INTERLEAVED_POINT_M = 12001,
    GEOARROW_TYPE_INTERLEAVED_LINESTRING_M = 12002,
    GEOARROW_TYPE_INTERLEAVED_POLYGON_M = 12003,
    GEOARROW_TYPE_INTERLEAVED_MULTIPOINT_M = 12004,
    GEOARROW_TYPE_INTERLEAVED_MULTILINESTRING_M = 12005,
    GEOARROW_TYPE_INTERLEAVED_MULTIPOLYGON_M = 12006,
    GEOARROW_TYPE_INTERLEAVED_GEOMETRYCOLLECTION_M = 12007,

    GEOARROW_TYPE_INTERLEAVED_POINT_ZM = 13001,
    GEOARROW_TYPE_INTERLEAVED_LINESTRING_ZM = 13002,
    GEOARROW_TYPE_INTERLEAVED_POLYGON_ZM = 13003,
    GEOARROW_TYPE_INTERLEAVED_MULTIPOINT_ZM = 13004,
    GEOARROW_TYPE_INTERLEAVED_MULTILINESTRING_ZM = 13005,
    GEOARROW_TYPE_INTERLEAVED_MULTIPOLYGON_ZM = 13006,
    GEOARROW_TYPE_INTERLEAVED_GEOMETRYCOLLECTION_ZM = 13007,
}

const GEOMETRY: i32 = 991;
const BOX: i32 = 990;

pub(crate) fn coord_type(code: i32) -> Result<CoordType, Error> {
    match code {
        1 => Ok(CoordType::Separated),
        2 => Ok(CoordType::Interleaved),
        _ => Err(Error::invalid(format!(
            "unsupported GeoArrowCoordType: {code}"
        ))),
    }
}

/// `Planar` is the GeoArrow default and is encoded as the *absence* of an
/// `edges` metadata key, hence `None` rather than a variant.
fn edges(code: i32) -> Result<Option<Edges>, Error> {
    match code {
        0 => Ok(None),
        1 => Ok(Some(Edges::Spherical)),
        2 => Ok(Some(Edges::Vincenty)),
        3 => Ok(Some(Edges::Thomas)),
        4 => Ok(Some(Edges::Andoyer)),
        5 => Ok(Some(Edges::Karney)),
        _ => Err(Error::invalid(format!(
            "unsupported GeoArrowEdgeType: {code}"
        ))),
    }
}

fn dimension(code: i32) -> Result<Dimension, Error> {
    match code {
        0 => Ok(Dimension::XY),
        1 => Ok(Dimension::XYZ),
        2 => Ok(Dimension::XYM),
        3 => Ok(Dimension::XYZM),
        _ => Err(Error::invalid(format!(
            "unsupported dimensions in GeoArrowType: {code}"
        ))),
    }
}

/// # Safety
/// `crs_projjson` must be null or a NUL-terminated UTF-8 string.
pub(crate) unsafe fn metadata(
    crs_projjson: *const c_char,
    edge_type: i32,
) -> Result<Arc<Metadata>, Error> {
    let crs = match unsafe { crs_projjson.as_ref() } {
        None => Crs::default(),
        Some(_) => {
            let text = unsafe { std::ffi::CStr::from_ptr(crs_projjson) }
                .to_str()
                .map_err(|e| Error::invalid(format!("crs is not valid UTF-8: {e}")))?;
            let json: serde_json::Value = serde_json::from_str(text)
                .map_err(|e| Error::invalid(format!("crs is not valid PROJJSON: {e}")))?;
            Crs::from_projjson(json)
        }
    };
    Ok(Arc::new(Metadata::new(crs, edges(edge_type)?)))
}

/// Decode a `GeoArrowType` value into the geoarrow-rs type it names.
///
/// geoarrow-c encodes the coordinate layout into the type value itself
/// (`GEOARROW_TYPE_INTERLEAVED_POINT`), so `coord_type` is consulted only for
/// the serialized and non-interleavable types where the value carries none.
pub(crate) fn data_type(code: i32, metadata: Arc<Metadata>) -> Result<RsType, Error> {
    if let Some(serialized) = serialized_type(code, &metadata) {
        return Ok(serialized);
    }
    let interleaved = code / 10000;
    let coords = match interleaved {
        0 => CoordType::Separated,
        1 => CoordType::Interleaved,
        _ => return Err(Error::invalid(format!("unsupported GeoArrowType: {code}"))),
    };
    let rest = code % 10000;

    if rest == GEOMETRY {
        return Ok(RsType::Geometry(
            GeometryType::new(metadata).with_coord_type(coords),
        ));
    }
    let dim = dimension(rest / 1000)?;
    if rest % 1000 == BOX {
        // geoarrow.box is always a struct of doubles, so an interleaved
        // spelling of it does not exist.
        if interleaved == 1 {
            return Err(Error::invalid(format!(
                "geoarrow.box has no interleaved form: {code}"
            )));
        }
        return Ok(RsType::Rect(BoxType::new(dim, metadata)));
    }
    Ok(match rest % 1000 {
        1 => RsType::Point(PointType::new(dim, metadata).with_coord_type(coords)),
        2 => RsType::LineString(LineStringType::new(dim, metadata).with_coord_type(coords)),
        3 => RsType::Polygon(PolygonType::new(dim, metadata).with_coord_type(coords)),
        4 => RsType::MultiPoint(MultiPointType::new(dim, metadata).with_coord_type(coords)),
        5 => {
            RsType::MultiLineString(MultiLineStringType::new(dim, metadata).with_coord_type(coords))
        }
        6 => RsType::MultiPolygon(MultiPolygonType::new(dim, metadata).with_coord_type(coords)),
        7 => RsType::GeometryCollection(
            GeometryCollectionType::new(dim, metadata).with_coord_type(coords),
        ),
        _ => return Err(Error::invalid(format!("unsupported GeoArrowType: {code}"))),
    })
}

fn serialized_type(code: i32, metadata: &Arc<Metadata>) -> Option<RsType> {
    let wkb = || WkbType::new(metadata.clone());
    let wkt = || WktType::new(metadata.clone());
    match code {
        100001 => Some(RsType::Wkb(wkb())),
        100002 => Some(RsType::LargeWkb(wkb())),
        100003 => Some(RsType::Wkt(wkt())),
        100004 => Some(RsType::LargeWkt(wkt())),
        100005 => Some(RsType::WkbView(wkb())),
        100006 => Some(RsType::WktView(wkt())),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode(code: i32) -> Result<RsType, Error> {
        data_type(code, Arc::new(Metadata::default()))
    }

    fn extension_name(code: i32) -> String {
        decode(code).unwrap().to_field("geometry", true).metadata()["ARROW:extension:name"].clone()
    }

    #[test]
    fn decodes_dimension_and_layout_from_the_type_value() {
        let cases = [
            (1, Dimension::XY, CoordType::Separated),
            (1003, Dimension::XYZ, CoordType::Separated),
            (2004, Dimension::XYM, CoordType::Separated),
            (3006, Dimension::XYZM, CoordType::Separated),
            (10001, Dimension::XY, CoordType::Interleaved),
            (13002, Dimension::XYZM, CoordType::Interleaved),
        ];
        for (code, dim, coords) in cases {
            let t = decode(code).unwrap();
            assert_eq!(t.dimension(), Some(dim), "dimension for {code}");
            assert_eq!(t.coord_type(), Some(coords), "coord type for {code}");
        }
    }

    #[test]
    fn decodes_every_declared_geometry_type() {
        for (code, expected) in [
            (1, "geoarrow.point"),
            (2, "geoarrow.linestring"),
            (3, "geoarrow.polygon"),
            (4, "geoarrow.multipoint"),
            (5, "geoarrow.multilinestring"),
            (6, "geoarrow.multipolygon"),
            (7, "geoarrow.geometrycollection"),
            (990, "geoarrow.box"),
            (991, "geoarrow.geometry"),
            (100001, "geoarrow.wkb"),
            (100003, "geoarrow.wkt"),
        ] {
            assert_eq!(extension_name(code), expected, "for {code}");
        }
    }

    /// The serialized types carry no dimension or layout, so their values must
    /// not be run through the arithmetic decomposition.
    #[test]
    fn serialized_types_ignore_the_layout_arithmetic() {
        for code in [100001, 100002, 100003, 100004, 100005, 100006] {
            assert!(decode(code).unwrap().dimension().is_none(), "for {code}");
        }
    }

    #[test]
    fn rejects_unknown_codes() {
        for code in [0, -1, 8, 999, 4001, 20001, 100007] {
            assert!(decode(code).is_err(), "expected {code} to be rejected");
        }
    }

    /// geoarrow.box is a struct of doubles; geoarrow-c has no interleaved
    /// spelling of it, so 10990 must not silently decode as a box.
    #[test]
    fn rejects_interleaved_box() {
        assert!(decode(10990).is_err());
    }

    #[test]
    fn rejects_unknown_coord_and_edge_codes() {
        for code in [0, 3, -1] {
            assert!(coord_type(code).is_err(), "coord type {code}");
        }
        for code in [6, -1] {
            assert!(edges(code).is_err(), "edge type {code}");
        }
        assert_eq!(edges(0).unwrap(), None);
    }
}
