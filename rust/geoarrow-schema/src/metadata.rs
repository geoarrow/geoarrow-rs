use arrow_schema::ArrowError;
use serde::{Deserialize, Serialize};

use crate::crs::Crs;
use crate::edges::Edges;

/// A GeoArrow metadata object following the extension metadata [defined by the GeoArrow
/// specification](https://geoarrow.org/extension-types).
///
/// This is serialized to JSON when a [`geoarrow`](self) array is exported to an [`arrow`] array and
/// deserialized when imported from an [`arrow`] array.
#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Metadata {
    // Raise the underlying crs fields to this level.
    // https://serde.rs/attr-flatten.html
    #[serde(flatten)]
    crs: Crs,

    /// If present, instructs consumers that edges follow a spherical path rather than a planar
    /// one. If this value is omitted, edges will be interpreted as planar.
    pub edges: Option<Edges>,
}

impl Metadata {
    /// Creates a new [`Metadata`] object.
    pub fn new(crs: Crs, edges: Option<Edges>) -> Self {
        Self { crs, edges }
    }

    /// Returns true if the metadata should be serialized.
    fn should_serialize(&self) -> bool {
        self.crs.should_serialize() || self.edges.is_some()
    }

    pub(crate) fn serialize(&self) -> Option<String> {
        if self.should_serialize() {
            Some(serde_json::to_string(&self).unwrap())
        } else {
            None
        }
    }

    pub(crate) fn deserialize(metadata: Option<&str>) -> Result<Self, ArrowError> {
        if let Some(ext_meta) = metadata {
            Ok(serde_json::from_str(ext_meta)
                .map_err(|err| ArrowError::ExternalError(Box::new(err)))?)
        } else {
            Ok(Default::default())
        }
    }
}
