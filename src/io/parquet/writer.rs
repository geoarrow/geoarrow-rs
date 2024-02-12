use std::io::Write;

use crate::error::Result;
use crate::table::GeoTable;

pub fn write_geoparquet<W: Write>(table: &mut GeoTable, writer: W) -> Result<()> {
    Ok(())
}
