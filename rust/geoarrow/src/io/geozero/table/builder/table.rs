use std::mem::replace;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use geozero::{FeatureProcessor, GeomProcessor, PropertyProcessor};

use crate::array::metadata::ArrayMetadata;
use crate::array::CoordType;
use crate::chunked_array::ChunkedNativeArrayDyn;
use geoarrow_schema::Dimension;
use crate::error::{GeoArrowError, Result};
use crate::io::geozero::table::builder::properties::PropertiesBatchBuilder;
use crate::table::Table;
use crate::trait_::{GeometryArrayBuilder, NativeArray};
use geo_traits::GeometryTrait;

/// Options for creating a GeoTableBuilder.
#[derive(Debug, Clone, PartialEq)]
pub struct GeoTableBuilderOptions {
    pub metadata: Arc<ArrayMetadata>,

    /// The [CoordType] of the generated geometry arrays
    pub coord_type: CoordType,

    /// Whether to prefer multi-geometries for geometries. Makes downcasting easier.
    pub prefer_multi: bool,

    /// The max number of rows in a batch
    pub batch_size: usize,

    /// If known, the schema of properties. Must not include the schema of the geometry.
    pub properties_schema: Option<SchemaRef>,

    /// The number of rows to be read
    pub num_rows: Option<usize>,
}

impl GeoTableBuilderOptions {
    pub fn new(
        coord_type: CoordType,
        prefer_multi: bool,
        batch_size: Option<usize>,
        properties_schema: Option<SchemaRef>,
        num_rows: Option<usize>,
        metadata: Arc<ArrayMetadata>,
    ) -> Self {
        Self {
            coord_type,
            prefer_multi,
            batch_size: batch_size.unwrap_or(65_536),
            properties_schema,
            num_rows,
            metadata,
        }
    }
}

impl Default for GeoTableBuilderOptions {
    fn default() -> Self {
        Self {
            coord_type: Default::default(),
            prefer_multi: true,
            batch_size: 65_536,
            properties_schema: None,
            num_rows: None,
            metadata: Default::default(),
        }
    }
}

// TODO:
// - This is schemaless, you need to validate that the schema doesn't change (maybe allow the user to pass in a schema?) and/or upcast data

/// A builder for creating a Table from a row-based source.
pub struct GeoTableBuilder<G: GeometryArrayBuilder + GeomProcessor> {
    /// The max number of rows in each batch
    ///
    /// not yet used.
    batch_size: usize,

    /// The total number of rows in the dataset to be read, including what has already been read
    total_num_rows: Option<usize>,

    /// Batches that have already been finished
    batches: Vec<RecordBatch>,

    /// The length of all batches that have already been finished
    batches_len: usize,

    /// Builder for the properties of the current batch
    prop_builder: PropertiesBatchBuilder,

    /// Geometry Array chunks that have already been finished
    /// This is kept separate so that schema resolution among batches can be handled without the
    /// geometry, and then the geometry column can be added at the end.
    geom_arrays: Vec<Arc<dyn NativeArray>>,

    /// Builder for the geometries of the current batch
    geom_builder: G,

    dim: Dimension,
}

impl<G: GeometryArrayBuilder + GeomProcessor> GeoTableBuilder<G> {
    pub fn new(dim: Dimension) -> Self {
        Self::new_with_options(dim, Default::default())
    }

    pub fn new_with_options(dim: Dimension, options: GeoTableBuilderOptions) -> Self {
        let (current_batch_size, num_batches) = if let Some(total_num_rows) = options.num_rows {
            (
                Some(total_num_rows.min(options.batch_size)),
                Some((total_num_rows as f64 / options.batch_size as f64).ceil() as usize),
            )
        } else {
            (None, None)
        };

        let prop_builder = match (options.properties_schema, current_batch_size) {
            (Some(schema), Some(batch_size)) => {
                PropertiesBatchBuilder::from_schema_with_capacity(&schema, batch_size)
            }
            (Some(schema), None) => PropertiesBatchBuilder::from_schema(&schema),
            (None, _) => PropertiesBatchBuilder::new(),
        };

        let (batches, geom_arrays) = if let Some(num_batches) = num_batches {
            (
                Vec::with_capacity(num_batches),
                Vec::with_capacity(num_batches),
            )
        } else {
            (vec![], vec![])
        };

        let geom_builder = if let Some(current_batch_size) = current_batch_size {
            G::with_geom_capacity_and_options(
                dim,
                current_batch_size,
                options.coord_type,
                options.metadata,
            )
        } else {
            G::with_geom_capacity_and_options(dim, 0, options.coord_type, options.metadata)
        };

        Self {
            batch_size: options.batch_size,
            total_num_rows: options.num_rows,
            batches,
            batches_len: 0,
            prop_builder,
            geom_arrays,
            geom_builder,
            dim,
        }
    }

    pub(crate) fn properties_builder_mut(&mut self) -> &mut PropertiesBatchBuilder {
        &mut self.prop_builder
    }

    /// Access the underlying geometry builder
    pub(crate) fn geom_builder(&mut self) -> &mut G {
        &mut self.geom_builder
    }

    pub fn push_geometry(&mut self, value: Option<&impl GeometryTrait<T = f64>>) -> Result<()> {
        self.geom_builder.push_geometry(value)
    }

    fn flush_batch(&mut self) -> geozero::error::Result<()> {
        let next_schema = self.prop_builder.schema();
        let coord_type = self.geom_builder.coord_type();
        let metadata = self.geom_builder.metadata();

        let (new_prop_builder, new_geom_builder) = if let Some(total_num_rows) = self.total_num_rows
        {
            let rows_left = total_num_rows - self.batches_len;
            let batch_size = self.batch_size.min(rows_left);
            let prop_builder =
                PropertiesBatchBuilder::from_schema_with_capacity(&next_schema, batch_size);
            let geom_builder =
                G::with_geom_capacity_and_options(self.dim, batch_size, coord_type, metadata);
            (prop_builder, geom_builder)
        } else {
            let prop_builder = PropertiesBatchBuilder::from_schema(&next_schema);
            let geom_builder = G::with_geom_capacity_and_options(self.dim, 0, coord_type, metadata);
            (prop_builder, geom_builder)
        };

        let existing_prop_builder = replace(&mut self.prop_builder, new_prop_builder);
        let existing_geom_builder = replace(&mut self.geom_builder, new_geom_builder);

        let batch = existing_prop_builder
            .finish()
            .expect("properties building failure");
        self.batches_len += batch.num_rows();
        self.batches.push(batch);

        let geom_array = existing_geom_builder.finish();
        self.geom_arrays.push(geom_array);

        Ok(())
    }

    pub fn finish(mut self) -> Result<Table> {
        // If there are rows that haven't flushed yet, flush them to batches
        if self.geom_builder.len() > 0 {
            self.flush_batch()?;
        }

        if self.batches.is_empty() {
            return Err(GeoArrowError::General("No rows loaded".to_string()));
        }

        // TODO: validate schema compatibility of batches and geometry arrays

        let batches = self.batches;
        let schema = batches[0].schema();
        let mut table = Table::try_new(batches, schema)?;

        let geom_slices = self
            .geom_arrays
            .iter()
            .map(|chunk| chunk.as_ref())
            .collect::<Vec<_>>();
        let geom_col = ChunkedNativeArrayDyn::from_geoarrow_chunks(&geom_slices)?.into_inner();
        let geom_field = geom_col.extension_field();

        table.append_column(geom_field, geom_col.array_refs())?;
        Ok(table)
    }
}

impl<G: GeometryArrayBuilder + GeomProcessor> Default for GeoTableBuilder<G> {
    fn default() -> Self {
        Self::new(Dimension::XY)
    }
}

impl<G: GeometryArrayBuilder + GeomProcessor> FeatureProcessor for GeoTableBuilder<G> {
    fn properties_end(&mut self) -> geozero::error::Result<()> {
        self.prop_builder.properties_end()
    }

    fn feature_end(&mut self, idx: u64) -> geozero::error::Result<()> {
        self.prop_builder.feature_end(idx)?;

        // If this finishes a batch, handle finish and provisioning a new batch
        // Note this has to be after prop_builder.feature_end for the len to be correct
        if self.prop_builder.len() >= self.batch_size {
            self.flush_batch()?;
        };

        Ok(())
    }
}

impl<G: GeometryArrayBuilder + GeomProcessor> PropertyProcessor for GeoTableBuilder<G> {
    fn property(
        &mut self,
        idx: usize,
        name: &str,
        value: &geozero::ColumnValue,
    ) -> geozero::error::Result<bool> {
        self.prop_builder.property(idx, name, value)
    }
}

impl<G: GeometryArrayBuilder + GeomProcessor> GeomProcessor for GeoTableBuilder<G> {
    fn xy(&mut self, x: f64, y: f64, idx: usize) -> geozero::error::Result<()> {
        self.geom_builder.xy(x, y, idx)
    }

    fn empty_point(&mut self, idx: usize) -> geozero::error::Result<()> {
        self.geom_builder.empty_point(idx)
    }

    fn point_begin(&mut self, idx: usize) -> geozero::error::Result<()> {
        self.geom_builder.point_begin(idx)
    }

    fn point_end(&mut self, idx: usize) -> geozero::error::Result<()> {
        self.geom_builder.point_end(idx)
    }

    fn multipoint_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        self.geom_builder.multipoint_begin(size, idx)
    }

    fn multipoint_end(&mut self, idx: usize) -> geozero::error::Result<()> {
        self.geom_builder.multipoint_end(idx)
    }

    fn linestring_begin(
        &mut self,
        tagged: bool,
        size: usize,
        idx: usize,
    ) -> geozero::error::Result<()> {
        self.geom_builder.linestring_begin(tagged, size, idx)
    }

    fn linestring_end(&mut self, tagged: bool, idx: usize) -> geozero::error::Result<()> {
        self.geom_builder.linestring_end(tagged, idx)
    }

    fn multilinestring_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        self.geom_builder.multilinestring_begin(size, idx)
    }

    fn multilinestring_end(&mut self, idx: usize) -> geozero::error::Result<()> {
        self.geom_builder.multilinestring_end(idx)
    }

    fn polygon_begin(
        &mut self,
        tagged: bool,
        size: usize,
        idx: usize,
    ) -> geozero::error::Result<()> {
        self.geom_builder.polygon_begin(tagged, size, idx)
    }

    fn polygon_end(&mut self, tagged: bool, idx: usize) -> geozero::error::Result<()> {
        self.geom_builder.polygon_end(tagged, idx)
    }

    fn multipolygon_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        self.geom_builder.multipolygon_begin(size, idx)
    }

    fn multipolygon_end(&mut self, idx: usize) -> geozero::error::Result<()> {
        self.geom_builder.multipolygon_end(idx)
    }

    fn geometrycollection_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        self.geom_builder.geometrycollection_begin(size, idx)
    }

    fn geometrycollection_end(&mut self, idx: usize) -> geozero::error::Result<()> {
        self.geom_builder.geometrycollection_end(idx)
    }
}
