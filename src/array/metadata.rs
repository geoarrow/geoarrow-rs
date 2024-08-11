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
    /// A JSON object describing the coordinate reference system (CRS) using PROJJSON. This key can
    /// also be omitted if the producer does not have any information about the CRS. Note that
    /// regardless of the axis order specified by the CRS, axis order will be interpreted according
    /// to the wording in the GeoPackage WKB binary encoding: axis order is always (longitude,
    /// latitude) and (easting, northing) regardless of the the axis order encoded in the CRS
    /// specification.
    pub crs: Option<Value>,

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
