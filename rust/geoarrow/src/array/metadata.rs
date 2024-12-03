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

    /// - `crs_type`: An optional string disambiguating the value of the `crs` field.
    ///   Must be omitted or a string value of:
    ///
    ///   - `"projjson"`: Indicates that the `"crs"` field was written as
    ///     [PROJJSON](https://proj.org/specifications/projjson.html).
    ///   - `"wkt2:2019"`: Indicates that the `"crs"` field was written as
    ///     [WKT2:2019](https://www.ogc.org/publications/standard/wkt-crs/).
    ///   - `"authority_code"`: Indicates that the `"crs"` field contains an identifier
    ///     in the form `AUTHORITY:CODE`. This should only be used as a last resort
    ///     (i.e., producers should prefer writing a complete description of the CRS).
    ///   - `"srid"`: Indicates that the `"crs"` field contains an opaque identifier
    ///     that requires the consumer to communicate with the producer outside of
    ///     this metadata. This should only be used as a last resort for database
    ///     drivers or readers that have no other option.
    ///
    ///   The `"crs_type"` should be omitted if the producer cannot guarantee the validity
    ///   of any of the above values (e.g., if it just serialized a CRS object
    ///   specifically into one of these representations).
    pub crs_type: Option<String>,

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
