use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Hash)]
pub enum Edges {
    #[serde(rename = "spherical")]
    Spherical,
}

#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq, Hash)]
pub struct ArrayMetadata {
    crs: Option<String>,
    edges: Option<Edges>,
}
