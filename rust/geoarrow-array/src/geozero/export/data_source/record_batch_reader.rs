use arrow_array::RecordBatchReader;
use arrow_schema::SchemaRef;

/// A newtype wrapper around an [`arrow_array::RecordBatchReader`] so that we can implement the
/// [`geozero::GeozeroDatasource`] trait on it.
///
/// This allows for exporting Arrow data to a geozero-based consumer even when not all of the Arrow
/// data is present in memory at once.
pub struct GeozeroRecordBatchReader(Box<dyn RecordBatchReader>);

impl GeozeroRecordBatchReader {
    /// Create a new GeozeroRecordBatchReader from a [`RecordBatchReader`].
    pub fn new(reader: Box<dyn RecordBatchReader>) -> Self {
        Self(reader)
    }

    /// Access the schema of this reader.
    pub fn schema(&self) -> SchemaRef {
        self.0.schema()
    }

    /// Access the underlying [`RecordBatchReader`].
    pub fn into_inner(self) -> Box<dyn RecordBatchReader> {
        self.0
    }
}

impl AsRef<Box<dyn RecordBatchReader>> for GeozeroRecordBatchReader {
    fn as_ref(&self) -> &Box<dyn RecordBatchReader> {
        &self.0
    }
}

impl AsMut<Box<dyn RecordBatchReader>> for GeozeroRecordBatchReader {
    fn as_mut(&mut self) -> &mut Box<dyn RecordBatchReader> {
        &mut self.0
    }
}

impl From<Box<dyn RecordBatchReader>> for GeozeroRecordBatchReader {
    fn from(value: Box<dyn RecordBatchReader>) -> Self {
        Self(value)
    }
}

impl From<Box<dyn RecordBatchReader + Send>> for GeozeroRecordBatchReader {
    fn from(value: Box<dyn RecordBatchReader + Send>) -> Self {
        Self(value)
    }
}
