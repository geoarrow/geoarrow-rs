//! Read from and write to [FlatGeobuf](https://flatgeobuf.org/) files.

mod reader;

pub use reader::{FlatGeobufReader, FlatGeobufReaderBuilder, FlatGeobufReaderOptions};
#[cfg(feature = "flatgeobuf_async")]
pub use reader::{FlatGeobufStream, FlatGeobufStreamBuilder, read_flatgeobuf_async};
