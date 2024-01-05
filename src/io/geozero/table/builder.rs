use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{OffsetSizeTrait, RecordBatch};
use arrow_schema::{Field, SchemaBuilder};
use geozero::{FeatureProcessor, GeomProcessor, PropertyProcessor};

use crate::algorithm::native::Downcast;
use crate::array::CoordType;
use crate::error::Result;
use crate::io::geozero::array::mixed::MixedGeometryStreamBuilder;
use crate::io::geozero::table::anyvalue::AnyBuilder;
use crate::table::GeoTable;
use crate::trait_::GeometryArrayTrait;

// TODO:
// - This is schemaless, you need to validate that the schema doesn't change (maybe allow the user to pass in a schema?) and/or upcast data
// - longer term: handle chunking during reading (num rows or num coords per batch)
pub struct GeoTableBuilder<O: OffsetSizeTrait> {
    // batch_size: usize,
    /// A mapping from column name to its builder
    columns: HashMap<String, AnyBuilder>,
    /// Row counter does not include the current row. So a row counter of 0 is expected if
    /// ingesting the first row.
    row_counter: usize,
    geometry: MixedGeometryStreamBuilder<O>,
}

impl<O: OffsetSizeTrait> GeoTableBuilder<O> {
    pub fn new() -> Self {
        Self::new_with_options(Default::default(), Default::default())
    }

    pub fn new_with_options(coord_type: CoordType, prefer_multi: bool) -> Self {
        Self {
            columns: HashMap::new(),
            row_counter: 0,
            geometry: MixedGeometryStreamBuilder::new_with_options(coord_type, prefer_multi),
        }
    }

    pub fn finish(self) -> Result<GeoTable> {
        // Set geometry column after property columns
        let geometry_column_index = self.columns.len();

        let mut columns = Vec::with_capacity(self.columns.len() + 1);
        let mut schema_builder = SchemaBuilder::with_capacity(self.columns.len() + 1);
        for (name, mut_column) in self.columns {
            let arr = mut_column.finish()?;
            schema_builder.push(Field::new(name, arr.data_type().clone(), true));
            columns.push(arr);
        }

        // Add geometry column and geometry field
        let geometry_column = self.geometry.finish();
        let geometry_field = geometry_column.extension_field();

        columns.push(geometry_column.into_array_ref());
        schema_builder.push(geometry_field);
        let schema = Arc::new(schema_builder.finish());

        let batch = RecordBatch::try_new(schema.clone(), columns)?;
        let table = GeoTable::try_new(schema, vec![batch], geometry_column_index)?;
        table.downcast(true)
    }
}

impl<O: OffsetSizeTrait> Default for GeoTableBuilder<O> {
    fn default() -> Self {
        Self::new()
    }
}

impl<O: OffsetSizeTrait> FeatureProcessor for GeoTableBuilder<O> {
    fn properties_end(&mut self) -> geozero::error::Result<()> {
        for (_name, col) in self.columns.iter_mut() {
            if col.len() == self.row_counter + 1 {
                // This is _expected_ when all columns were visited
                continue;
            }

            // This can happen if a column did not have a value in this row, such as if the
            // properties keys in GeoJSON change per row.
            if col.len() == self.row_counter {
                col.append_null();
            } else {
                panic!("unexpected length");
            }
        }

        Ok(())
    }

    fn feature_end(&mut self, idx: u64) -> geozero::error::Result<()> {
        debug_assert_eq!(idx as usize, self.row_counter);
        self.row_counter += 1;
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

    fn polygon_begin(
        &mut self,
        tagged: bool,
        size: usize,
        idx: usize,
    ) -> geozero::error::Result<()> {
        self.geometry.polygon_begin(tagged, size, idx)
    }

    fn multipolygon_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        self.geometry.multipolygon_begin(size, idx)
    }
}
