use geozero::geojson::GeoJsonReader;
use geozero::GeozeroDatasource;
use std::io::Read;

use crate::error::Result;
use crate::io::geozero::table::builder::GeoTableBuilder;
use crate::table::GeoTable;

pub fn read_geojson<R: Read>(reader: R) -> Result<GeoTable> {
    let mut geojson = GeoJsonReader(reader);
    let mut geo_table = GeoTableBuilder::<i32>::new();
    geojson.process(&mut geo_table)?;
    geo_table.finish()
}
