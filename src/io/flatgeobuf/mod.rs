//! Contains implementations of reading from and writing to [FlatGeobuf](https://flatgeobuf.org/)
//! files.

mod reader;
mod writer;

pub use reader::read_flatgeobuf;
pub use writer::{write_flatgeobuf, write_flatgeobuf_with_options};
