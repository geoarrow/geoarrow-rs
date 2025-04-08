//! Read from and write to [FlatGeobuf](https://flatgeobuf.org/) files.

mod reader;
mod writer;

pub use reader::{FlatGeobufReader, FlatGeobufReaderBuilder, FlatGeobufReaderOptions};
#[cfg(feature = "flatgeobuf_async")]
pub use reader::{FlatGeobufStream, FlatGeobufStreamBuilder, read_flatgeobuf_async};
pub use writer::{FlatGeobufWriterOptions, write_flatgeobuf, write_flatgeobuf_with_options};
