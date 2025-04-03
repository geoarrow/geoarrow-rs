//! Metadata contained within a GeoArrow array.
//!
//! This metadata is [defined by the GeoArrow specification](https://geoarrow.org/extension-types).

// impl TryFrom<&Field> for ArrayMetadata {
//     type Error = GeoArrowError;

//     fn try_from(value: &Field) -> Result<Self, Self::Error> {
//         if let Some(ext_meta) = value.metadata().get("ARROW:extension:metadata") {
//             Ok(serde_json::from_str(ext_meta)?)
//         } else {
//             Ok(Default::default())
//         }
//     }
// }
