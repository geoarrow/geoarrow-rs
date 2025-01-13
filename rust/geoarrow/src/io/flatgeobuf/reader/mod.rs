#[cfg(feature = "flatgeobuf_async")]
mod r#async;
mod common;
#[cfg(feature = "flatgeobuf_async")]
mod object_store_reader;
mod sync;

pub use common::FlatGeobufReaderOptions;
#[cfg(feature = "flatgeobuf_async")]
pub use r#async::{FlatGeobufStreamBuilder, FlatGeobufStream};
pub use sync::{FlatGeobufReader, FlatGeobufReaderBuilder};
