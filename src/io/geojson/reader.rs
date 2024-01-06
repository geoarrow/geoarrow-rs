use geozero::geojson::GeoJsonReader;
use geozero::GeozeroDatasource;
use std::io::Read;

use crate::array::CoordType;
use crate::error::Result;
use crate::io::geozero::array::mixed::MixedGeometryStreamBuilder;
use crate::io::geozero::table::builder::{GeoTableBuilderOptions, GeoTableBuilder};
use crate::table::GeoTable;

pub fn read_geojson<R: Read>(reader: R, batch_size: Option<usize>) -> Result<GeoTable> {
    let mut geojson = GeoJsonReader(reader);
    let options = GeoTableBuilderOptions::new(CoordType::Interleaved, true, batch_size, None, None);
    let mut geo_table = GeoTableBuilder::<MixedGeometryStreamBuilder<i32>>::new_with_options(options);
    geojson.process(&mut geo_table)?;
    geo_table.finish()
}

#[cfg(test)]
mod test {
    use std::fs::File;
    use std::io::BufReader;

    use super::*;

    #[ignore = "non-vendored file"]
    #[test]
    fn test_read_geojson() {
        let path = "/Users/kyle/Downloads/UScounties.geojson";
        let mut filein = BufReader::new(File::open(path).unwrap());
        let _table = read_geojson(&mut filein, None).unwrap();
    }
}
