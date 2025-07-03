use std::collections::HashMap;

use geoarrow_schema::crs::CrsTransform;

/// Allowed encodings when writing to GeoParquet
#[derive(Copy, Clone, Default)]
#[allow(clippy::upper_case_acronyms)]
pub enum GeoParquetWriterEncoding {
    /// Well-known binary geometry encoding
    ///
    /// This is the only encoding supported in GeoParquet version 1.0, so if you wish to maintain
    /// compatibility with that version, you must choose WKB.
    #[default]
    WKB,

    /// GeoArrow-native encoding. This is supported as of GeoParquet version 1.1.
    GeoArrow,
}

/// Container for column properties that can be changed as part of writer.
///
/// If a field is `None`, it means that no specific value has been set for this column,
/// so some subsequent or default value must be used.
#[derive(Default)]
pub(crate) struct ColumnOptions {
    pub(crate) encoding: Option<GeoParquetWriterEncoding>,
    pub(crate) generate_covering: Option<bool>,
    pub(crate) covering_prefix: Option<String>,
}

impl ColumnOptions {
    fn set_encoding(&mut self, value: GeoParquetWriterEncoding) {
        self.encoding = Some(value);
    }

    fn set_generate_covering(&mut self, value: bool) {
        self.generate_covering = Some(value);
    }

    fn set_covering_prefix(&mut self, value: String) {
        self.covering_prefix = Some(value);
    }
}

/// Builder for [`GeoParquetWriterOptions`]
#[derive(Default)]
pub struct GeoParquetWriterOptionsBuilder {
    /// Primary geometry column name.
    primary_column: Option<String>,
    crs_transform: Option<Box<dyn CrsTransform>>,
    default_column_properties: ColumnOptions,
    column_properties: HashMap<String, ColumnOptions>,
}

impl GeoParquetWriterOptionsBuilder {
    /// Set the name of the primary geometry column.
    ///
    /// If not set, if `"geometry`  or `"geography"` exist as a column name, either of those (in
    /// that order) will be chosen as the primary geometry column. Otherwise, all geometry columns
    /// will be sorted by name, and the first will be chosen.
    pub fn set_primary_column(mut self, col: String) -> Self {
        self.primary_column = Some(col);
        self
    }

    /// Set the [`CrsTransform`].
    ///
    /// This is an implementation for converting CRS from the GeoArrow representation to PROJJSON,
    /// which is the representation required by GeoParquet.
    pub fn set_crs_transform(mut self, crs_transform: Box<dyn CrsTransform>) -> Self {
        self.crs_transform = Some(crs_transform);
        self
    }

    /// Helper method to get existing or new mutable reference of column properties.
    #[inline]
    fn get_mut_props(&mut self, col: String) -> &mut ColumnOptions {
        self.column_properties.entry(col).or_default()
    }

    /// Set the default status for whether all geometry columns should have a covering generated
    /// for them.
    pub fn set_generate_covering(mut self, value: bool) -> Self {
        self.default_column_properties.set_generate_covering(value);
        self
    }

    /// Set whether a specific geometry column should have a covering generated for it.
    pub fn set_column_generate_covering(mut self, col: String, value: bool) -> Self {
        self.get_mut_props(col).set_generate_covering(value);
        self
    }

    /// Set the string prefix that will be used for the covering column name.
    ///
    /// If not set, the default is `"bbox_"` for a column named `"geometry"` or `"geography"`. For
    /// any other column, the default is `format!("{column_name}_bbox_")`.
    pub fn set_column_covering_prefix(mut self, col: String, value: String) -> Self {
        self.get_mut_props(col).set_covering_prefix(value);
        self
    }

    /// Set the default encoding for all geometry columns.
    pub fn set_encoding(mut self, value: GeoParquetWriterEncoding) -> Self {
        self.default_column_properties.set_encoding(value);
        self
    }

    /// Set the encoding for a specific geometry column.
    pub fn set_column_encoding(mut self, col: String, value: GeoParquetWriterEncoding) -> Self {
        self.get_mut_props(col).set_encoding(value);
        self
    }
}

/// Options for writing GeoParquet
#[derive(Default)]
pub struct GeoParquetWriterOptions {
    pub(crate) primary_column: Option<String>,
    pub(crate) crs_transform: Option<Box<dyn CrsTransform>>,
    pub(crate) default_column_properties: ColumnOptions,
    pub(crate) column_properties: HashMap<String, ColumnOptions>,
}
