// #[cfg(feature = "async")]
// mod r#async;
mod common;
// #[cfg(feature = "async")]
// mod object_store_reader;
mod sync;
mod table_builder;

// #[cfg(feature = "async")]
// pub use r#async::{FlatGeobufStream, FlatGeobufStreamBuilder, read_flatgeobuf_async};
pub use common::FlatGeobufReaderOptions;
pub use sync::{FlatGeobufReaderBuilder, FlatGeobufRecordBatchIterator};
