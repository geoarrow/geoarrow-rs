use arrow_wasm::Table;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct GeoTable(pub(crate) geoarrow::table::GeoTable);

#[wasm_bindgen]
impl GeoTable {
    /// Consume this GeoTable and convert into a non-spatial Arrow Table
    #[wasm_bindgen(js_name = intoTable)]
    pub fn into_table(self) -> Table {
        let (schema, batches, _) = self.0.into_inner();
        Table::new(schema, batches)
    }
}

impl From<geoarrow::table::GeoTable> for GeoTable {
    fn from(value: geoarrow::table::GeoTable) -> Self {
        Self(value)
    }
}

impl From<GeoTable> for geoarrow::table::GeoTable {
    fn from(value: GeoTable) -> Self {
        value.0
    }
}
