//! Read from and write to [FlatGeobuf](https://flatgeobuf.org/) files.

mod object_store_reader;
mod reader;
mod reader_async;
mod writer;

pub use reader::read_flatgeobuf;
pub use writer::{write_flatgeobuf, write_flatgeobuf_with_options};
