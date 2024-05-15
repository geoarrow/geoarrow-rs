use crate::error::GeoArrowError;
use crate::table::Table;
use arrow_array::{RecordBatchIterator, RecordBatchReader as _RecordBatchReader};
use arrow_schema::SchemaRef;

/// A wrapper around an [arrow_array::RecordBatchReader] so that we can impl the GeozeroDatasource
/// trait.
pub struct RecordBatchReader(Option<Box<dyn _RecordBatchReader>>);

impl RecordBatchReader {
    pub fn new(reader: Box<dyn _RecordBatchReader>) -> Self {
        Self(Some(reader))
    }

    pub fn schema(&self) -> Result<SchemaRef, GeoArrowError> {
        let reader = self
            .0
            .as_ref()
            .ok_or(GeoArrowError::General("Closed stream".to_string()))?;
        Ok(reader.schema())
    }

    pub fn take(&mut self) -> Option<Box<dyn _RecordBatchReader>> {
        self.0.take()
    }
}

impl From<Table> for RecordBatchReader {
    fn from(value: Table) -> Self {
        let (schema, batches) = value.into_inner();
        Self(Some(Box::new(RecordBatchIterator::new(
            batches.into_iter().map(Ok),
            schema,
        ))))
    }
}

impl From<Box<dyn _RecordBatchReader>> for RecordBatchReader {
    fn from(value: Box<dyn _RecordBatchReader>) -> Self {
        Self(Some(value))
    }
}

impl From<Box<dyn _RecordBatchReader + Send>> for RecordBatchReader {
    fn from(value: Box<dyn _RecordBatchReader + Send>) -> Self {
        Self(Some(value))
    }
}
