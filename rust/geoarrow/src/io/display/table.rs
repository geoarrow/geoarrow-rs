use std::fmt;

use crate::table::Table;

impl fmt::Display for Table {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Table")?;
        for field in self.schema().fields() {
            writeln!(f, "{}: {}", field.name(), field.data_type())?;
        }
        Ok(())
    }
}
