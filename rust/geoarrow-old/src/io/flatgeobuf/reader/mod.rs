#[cfg(feature = "flatgeobuf_async")]
mod r#async;
mod common;
#[cfg(feature = "flatgeobuf_async")]
mod object_store_reader;
mod sync;

#[cfg(feature = "flatgeobuf_async")]
pub use r#async::{FlatGeobufStream, FlatGeobufStreamBuilder, read_flatgeobuf_async};
pub use common::FlatGeobufReaderOptions;
pub use sync::{FlatGeobufReader, FlatGeobufReaderBuilder};
