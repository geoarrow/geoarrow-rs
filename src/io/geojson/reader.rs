use geozero::geojson::GeoJsonReader;
use geozero::GeozeroDatasource;
use std::io::Read;

use crate::array::CoordType;
use crate::error::Result;
use crate::io::geozero::table::builder::GeoTableBuilder;
use crate::table::GeoTable;

pub fn read_geojson<R: Read>(reader: R) -> Result<GeoTable> {
    let mut geojson = GeoJsonReader(reader);
    let mut geo_table = GeoTableBuilder::<i32>::new_with_options(CoordType::Interleaved, true);
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
        let _table = read_geojson(&mut filein).unwrap();
    }
}
