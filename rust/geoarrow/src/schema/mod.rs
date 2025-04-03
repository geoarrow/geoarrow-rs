//! Geospatial operations on Arrow schemas

use arrow_schema::extension::EXTENSION_TYPE_NAME_KEY;

use crate::table::GEOARROW_EXTENSION_NAMES;

/// Extra geospatial-specific functionality on Arrow schemas
pub trait GeoSchemaExt {
    /// Find the indices of all geometry columns in this schema.
    ///
    /// The returned `Vec` may be empty if the table contains no geometry columns, or it may
    /// contain more than one element if the table contains multiple tagged geometry columns.
    fn geometry_columns(&self) -> Vec<usize>;
}

impl GeoSchemaExt for &arrow_schema::Schema {
    fn geometry_columns(&self) -> Vec<usize> {
        let mut geom_indices = vec![];
        for (field_idx, field) in self.fields().iter().enumerate() {
            let meta = field.metadata();
            if let Some(ext_name) = meta.get(EXTENSION_TYPE_NAME_KEY) {
                if GEOARROW_EXTENSION_NAMES.contains(ext_name.as_str()) {
                    geom_indices.push(field_idx);
                }
            }
        }
        geom_indices
    }
}
