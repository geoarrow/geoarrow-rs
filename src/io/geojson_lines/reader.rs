use geozero::geojson::GeoJsonLineReader;
use geozero::GeozeroDatasource;
use std::io::BufRead;

use crate::array::CoordType;
use crate::error::Result;
use crate::io::geozero::array::MixedGeometryStreamBuilder;
use crate::io::geozero::table::{GeoTableBuilder, GeoTableBuilderOptions};
use crate::table::GeoTable;

/// Read a GeoJSON Lines file
///
/// This expects a GeoJSON Feature on each line of a text file, with a newline character separating
/// each Feature.
pub fn read_geojson_lines<R: BufRead>(reader: R, batch_size: Option<usize>) -> Result<GeoTable> {
    let mut geojson_line_reader = GeoJsonLineReader::new(reader);

    // TODO: set crs to epsg:4326?
    let options = GeoTableBuilderOptions::new(
        CoordType::Interleaved,
        true,
        batch_size,
        None,
        None,
        Default::default(),
    );
    let mut geo_table =
        GeoTableBuilder::<MixedGeometryStreamBuilder<i32>>::new_with_options(options);
    geojson_line_reader.process(&mut geo_table)?;
    geo_table.finish()
}
