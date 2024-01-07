use geozero::geojson::GeoJsonReader;
use geozero::GeozeroDatasource;
use std::io::{BufRead, Cursor};

use crate::array::CoordType;
use crate::error::Result;
use crate::io::geozero::array::mixed::MixedGeometryStreamBuilder;
use crate::io::geozero::table::builder::{GeoTableBuilder, GeoTableBuilderOptions};
use crate::table::GeoTable;

/// Read a GeoJSON Lines file
///
/// This expects a GeoJSON Feature on each line of a text file, with a newline character separating
/// each Feature.
pub fn read_geojson_lines<R: BufRead>(reader: R, batch_size: Option<usize>) -> Result<GeoTable> {
    let options = GeoTableBuilderOptions::new(CoordType::Interleaved, true, batch_size, None, None);
    let mut geo_table =
        GeoTableBuilder::<MixedGeometryStreamBuilder<i32>>::new_with_options(options);

    for line in reader.lines() {
        let mut geojson = GeoJsonReader(Cursor::new(line?));
        geojson.process(&mut geo_table)?;
    }

    geo_table.finish()
}
