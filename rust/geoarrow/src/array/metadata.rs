//! Metadata contained within a GeoArrow array.
//!
//! This metadata is [defined by the GeoArrow specification](https://geoarrow.org/extension-types).

use arrow_schema::Field;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::GeoArrowError;

/// If present, instructs consumers that edges follow a spherical path rather than a planar one. If
/// this value is omitted, edges will be interpreted as planar.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Edges {
    /// Follow a spherical path rather than a planar.
    ///
    /// See [the geoarrow
    /// specification](https://github.com/geoarrow/geoarrow/blob/main/extension-types.md#extension-metadata)
    /// for more information aobut how `edges` should be used.
    #[serde(rename = "spherical")]
    Spherical,
}

/// An optional string disambiguating the value of the `crs` field.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum CRSType {
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

/// A GeoArrow metadata object following the extension metadata [defined by the GeoArrow
/// specification](https://geoarrow.org/extension-types).
///
/// This is serialized to JSON when a [`geoarrow`](self) array is exported to an [`arrow`] array and
/// deserialized when imported from an [`arrow`] array.
#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ArrayMetadata {
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
    pub crs: Option<Value>,

    /// An optional string disambiguating the value of the `crs` field.
    ///
    /// The `"crs_type"` should be omitted if the producer cannot guarantee the validity
    /// of any of the above values (e.g., if it just serialized a CRS object
    /// specifically into one of these representations).
    pub crs_type: Option<CRSType>,

    /// If present, instructs consumers that edges follow a spherical path rather than a planar
    /// one. If this value is omitted, edges will be interpreted as planar.
    pub edges: Option<Edges>,
}

impl ArrayMetadata {
    /// Decide whether this [ArrayMetadata] should be written to Arrow metadata (aka if it is
    /// non-empty)
    pub fn should_serialize(&self) -> bool {
        self.crs.is_some() || self.edges.is_some()
    }

    /// Construct from a PROJJSON object.
    ///
    /// Note that `value` should be a _parsed_ JSON object; this should not contain
    /// `Value::String`.
    pub fn from_projjson(value: Value) -> Self {
        Self::default().with_projjson(value)
    }

    /// Construct from a WKT:2019 string.
    pub fn from_wkt2_2019(value: String) -> Self {
        Self::default().with_wkt2_2019(value)
    }

    /// Construct from an opaque string.
    pub fn from_unknown_crs_type(value: String) -> Self {
        Self::default().with_unknown_crs_type(value)
    }

    /// Construct from an authority:code string.
    pub fn from_authority_code(value: String) -> Self {
        Self::default().with_authority_code(value)
    }

    /// Set the CRS using a PROJJSON object.
    ///
    /// Note that `value` should be a _parsed_ JSON object; this should not contain
    /// `Value::String`.
    pub fn with_projjson(mut self, value: Value) -> Self {
        self.crs = Some(value);
        self.crs_type = Some(CRSType::Projjson);
        self
    }

    /// Set the CRS using a WKT:2019 string.
    pub fn with_wkt2_2019(mut self, value: String) -> Self {
        self.crs = Some(Value::String(value));
        self.crs_type = Some(CRSType::Wkt2_2019);
        self
    }

    /// Set the CRS using an opaque string.
    pub fn with_unknown_crs_type(mut self, value: String) -> Self {
        self.crs = Some(Value::String(value));
        self.crs_type = None;
        self
    }

    /// Set the CRS using an authority:code string.
    pub fn with_authority_code(mut self, value: String) -> Self {
        assert!(value.contains(':'), "':' should be authority:code CRS");
        self.crs = Some(Value::String(value));
        self.crs_type = Some(CRSType::AuthorityCode);
        self
    }

    /// Set the edge type.
    pub fn with_edges(mut self, edges: Edges) -> Self {
        self.edges = Some(edges);
        self
    }
}

impl TryFrom<&Field> for ArrayMetadata {
    type Error = GeoArrowError;

    fn try_from(value: &Field) -> Result<Self, Self::Error> {
        if let Some(ext_meta) = value.metadata().get("ARROW:extension:metadata") {
            Ok(serde_json::from_str(ext_meta)?)
        } else {
            Ok(Default::default())
        }
    }
}
