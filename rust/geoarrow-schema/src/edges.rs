use serde::{Deserialize, Serialize};

/// If present, instructs consumers that edges follow a spherical path rather than a planar one. If
/// this value is omitted, edges will be interpreted as planar.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum Edges {
    /// Follow a spherical path rather than a planar.
    ///
    /// See [the geoarrow
    /// specification](https://github.com/geoarrow/geoarrow/blob/main/extension-types.md#extension-metadata)
    /// for more information about how `edges` should be used.
    #[serde(rename = "spherical")]
    Spherical,
}
