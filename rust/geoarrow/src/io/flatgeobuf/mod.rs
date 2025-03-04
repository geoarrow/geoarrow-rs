//! Read from and write to [FlatGeobuf](https://flatgeobuf.org/) files.

mod reader;
mod writer;

#[cfg(feature = "flatgeobuf_async")]
pub use reader::{read_flatgeobuf_async, FlatGeobufStream, FlatGeobufStreamBuilder};
pub use reader::{FlatGeobufReader, FlatGeobufReaderBuilder, FlatGeobufReaderOptions};
pub use writer::{write_flatgeobuf, write_flatgeobuf_with_options, FlatGeobufWriterOptions};
