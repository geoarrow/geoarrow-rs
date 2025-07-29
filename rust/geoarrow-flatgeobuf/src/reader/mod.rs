#[cfg(feature = "async")]
mod r#async;
mod common;
#[cfg(feature = "object_store")]
pub mod object_store;
mod sync;
mod table_builder;

#[cfg(feature = "async")]
pub use r#async::FlatGeobufRecordBatchStream;
pub use common::{FlatGeobufReaderOptions, parse_header};
pub use sync::FlatGeobufRecordBatchIterator;
