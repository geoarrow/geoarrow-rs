use std::collections::HashMap;

use arrow_array::OffsetSizeTrait;
use geozero::{FeatureProcessor, GeomProcessor, PropertyProcessor};

use crate::io::geozero::array::mixed::MixedGeometryStreamBuilder;
use crate::io::geozero::table::anyvalue::AnyBuilder;
use crate::table::GeoTable;

// TODO:
// - This is schemaless, you need to validate that the schema doesn't change (maybe allow the user to pass in a schema?) and/or upcast data
// - longer term: handle chunking during reading (num rows or num coords per batch)
pub struct GeoTableBuilder<O: OffsetSizeTrait> {
    // batch_size: usize,
    /// A mapping from column name to its builder
    columns: HashMap<String, AnyBuilder>,
    row_counter: usize,
    geometry: MixedGeometryStreamBuilder<O>,
}

impl<O: OffsetSizeTrait> GeoTableBuilder<O> {
    pub fn new() -> Self {
        Self {
            columns: HashMap::new(),
            row_counter: 0,
            geometry: MixedGeometryStreamBuilder::new(),
        }
    }

    pub fn finish(self) -> GeoTable {
        todo!()
    }
}

impl<O: OffsetSizeTrait> Default for GeoTableBuilder<O> {
    fn default() -> Self {
        Self::new()
    }
}

impl<O: OffsetSizeTrait> FeatureProcessor for GeoTableBuilder<O> {
    fn properties_end(&mut self) -> geozero::error::Result<()> {
        // TODO: if any columns in `columns` _weren't_ visited, add null values
        Ok(())
    }
}

impl<O: OffsetSizeTrait> PropertyProcessor for GeoTableBuilder<O> {
    fn property(
        &mut self,
        // TODO: is this the row? Is this the positional index within the column?
        _idx: usize,
        name: &str,
        value: &geozero::ColumnValue,
    ) -> geozero::error::Result<bool> {
        if let Some(any_builder) = self.columns.get_mut(name) {
            any_builder.add_value(value);
        } else {
            // If this column name doesn't yet exist
            let builder = AnyBuilder::from_value_prefill(value, self.row_counter);
            self.columns.insert(name.to_string(), builder);
        }
        Ok(false)
    }
}

impl<O: OffsetSizeTrait> GeomProcessor for GeoTableBuilder<O> {
    fn xy(&mut self, x: f64, y: f64, idx: usize) -> geozero::error::Result<()> {
        self.geometry.xy(x, y, idx)
    }

    fn empty_point(&mut self, idx: usize) -> geozero::error::Result<()> {
        self.geometry.empty_point(idx)
    }

    fn point_begin(&mut self, idx: usize) -> geozero::error::Result<()> {
        self.geometry.empty_point(idx)
    }

    fn multipoint_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        self.geometry.multipoint_begin(size, idx)
    }

    fn linestring_begin(
        &mut self,
        tagged: bool,
        size: usize,
        idx: usize,
    ) -> geozero::error::Result<()> {
        self.geometry.linestring_begin(tagged, size, idx)
    }

    fn multilinestring_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        self.geometry.multilinestring_begin(size, idx)
    }

    fn multipolygon_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        self.geometry.multipolygon_begin(size, idx)
    }
}
