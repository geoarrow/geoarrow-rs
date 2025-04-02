use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Crs {
    /// One of:
    ///
    /// - A JSON object describing the coordinate reference system (CRS)
    ///   using [PROJJSON](https://proj.org/specifications/projjson.html).
    /// - A string containing a serialized CRS representation. This option
    ///   is intended as a fallback for producers (e.g., database drivers or
    ///   file readers) that are provided a CRS in some form but do not have the
    ///   means to convert it to PROJJSON.
    /// - Omitted, indicating that the producer does not have any information about
    ///   the CRS.
    ///
    /// For maximum compatibility, producers should write PROJJSON where possible.
    /// Note that regardless of the axis order specified by the CRS, axis order will be interpreted
    /// [GeoPackage WKB binary encoding](https://www.geopackage.org/spec130/index.html#gpb_format):
    /// axis order is always (longitude, latitude) and (easting, northing)
    /// regardless of the the axis order encoded in the CRS specification.
    crs: Option<Value>,

    /// An optional string disambiguating the value of the `crs` field.
    ///
    /// The `"crs_type"` should be omitted if the producer cannot guarantee the validity
    /// of any of the above values (e.g., if it just serialized a CRS object
    /// specifically into one of these representations).
    crs_type: Option<CrsType>,
}

impl Crs {
    /// Construct from a PROJJSON object.
    ///
    /// Note that `value` should be a _parsed_ JSON object; this should not contain
    /// `Value::String`.
    pub fn from_projjson(value: Value) -> Self {
        Self {
            crs: Some(value),
            crs_type: Some(CrsType::Projjson),
        }
    }

    /// Construct from a WKT:2019 string.
    pub fn from_wkt2_2019(value: String) -> Self {
        Self {
            crs: Some(Value::String(value)),
            crs_type: Some(CrsType::Wkt2_2019),
        }
    }

    /// Construct from an opaque string.
    pub fn from_unknown_crs_type(value: String) -> Self {
        Self {
            crs: Some(Value::String(value)),
            crs_type: None,
        }
    }

    /// Construct from an authority:code string.
    pub fn from_authority_code(value: String) -> Self {
        assert!(value.contains(':'), "':' should be authority:code CRS");
        Self {
            crs: Some(Value::String(value)),
            crs_type: Some(CrsType::AuthorityCode),
        }
    }

    /// Return `true` if we should include a CRS key in the GeoArrow metadata
    pub(crate) fn should_serialize(&self) -> bool {
        self.crs.is_some()
    }
}

/// An optional string disambiguating the value of the `crs` field.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum CrsType {
    /// Indicates that the `"crs"` field was written as
    /// [PROJJSON](https://proj.org/specifications/projjson.html).
    #[serde(rename = "projjson")]
    Projjson,

    /// Indicates that the `"crs"` field was written as
    /// [WKT2:2019](https://www.ogc.org/publications/standard/wkt-crs/).
    #[serde(rename = "wkt2:2019")]
    Wkt2_2019,

    /// Indicates that the `"crs"` field contains an identifier
    /// in the form `AUTHORITY:CODE`. This should only be used as a last resort
    /// (i.e., producers should prefer writing a complete description of the CRS).
    #[serde(rename = "authority_code")]
    AuthorityCode,

    /// Indicates that the `"crs"` field contains an opaque identifier
    /// that requires the consumer to communicate with the producer outside of
    /// this metadata. This should only be used as a last resort for database
    /// drivers or readers that have no other option.
    #[serde(rename = "srid")]
    Srid,
}
