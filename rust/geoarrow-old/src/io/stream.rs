use crate::error::GeoArrowError;
use crate::table::Table;
use arrow_array::{RecordBatchIterator, RecordBatchReader as _RecordBatchReader};
use arrow_schema::SchemaRef;

/// A newtype wrapper around an [`arrow_array::RecordBatchReader`] so that we can implement the
/// [`geozero::GeozeroDatasource`] trait on it.
///
/// This allows for exporting Arrow data to a geozero-based consumer even when not all of the Arrow
/// data is present in memory at once.
pub struct RecordBatchReader(Box<dyn _RecordBatchReader>);

impl RecordBatchReader {
    /// Create a new RecordBatchReader from an [`arrow_array::RecordBatchReader`].
    pub fn new(reader: Box<dyn _RecordBatchReader>) -> Self {
        Self(reader)
    }

    /// Access the schema of this reader.
    pub fn schema(&self) -> SchemaRef {
        self.0.schema()
    }

    /// Access a mutable reference to the underlying [`arrow_array::RecordBatchReader`].
    pub fn inner_mut(&mut self) -> &mut Box<dyn _RecordBatchReader> {
        &mut self.0
    }

    /// Access the underlying [`arrow_array::RecordBatchReader`].
    pub fn into_inner(self) -> Box<dyn _RecordBatchReader> {
        self.0
    }
}

impl From<Table> for RecordBatchReader {
    fn from(value: Table) -> Self {
        let (batches, schema) = value.into_inner();
        Self(Box::new(RecordBatchIterator::new(
            batches.into_iter().map(Ok),
            schema,
        )))
    }
}

impl From<&Table> for RecordBatchReader {
    fn from(value: &Table) -> Self {
        value.clone().into()
    }
}

impl TryFrom<RecordBatchReader> for Table {
    type Error = GeoArrowError;

    fn try_from(value: RecordBatchReader) -> Result<Self, Self::Error> {
        let reader = value.0;
        let schema = reader.schema();
        Table::try_new(reader.collect::<Result<_, _>>()?, schema)
    }
}

impl From<Box<dyn _RecordBatchReader>> for RecordBatchReader {
    fn from(value: Box<dyn _RecordBatchReader>) -> Self {
        Self(value)
    }
}

impl From<Box<dyn _RecordBatchReader + Send>> for RecordBatchReader {
    fn from(value: Box<dyn _RecordBatchReader + Send>) -> Self {
        Self(value)
    }
}
