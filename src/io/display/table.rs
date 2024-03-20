use std::fmt;

use crate::table::GeoTable;

impl fmt::Display for GeoTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "GeoTable")?;
        for field in self.schema().fields() {
            writeln!(f, "{}: {}", field.name(), field.data_type())?;
        }
        Ok(())
    }
}
