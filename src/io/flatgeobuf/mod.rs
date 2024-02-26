//! Read from and write to [FlatGeobuf](https://flatgeobuf.org/) files.

mod reader;
mod writer;

pub use reader::{read_flatgeobuf, read_flatgeobuf_async};
pub use writer::{write_flatgeobuf, write_flatgeobuf_with_options};
