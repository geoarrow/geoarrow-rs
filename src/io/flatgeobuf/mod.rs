//! Read the [FlatGeobuf](https://flatgeobuf.org/) format.

mod reader;
mod writer;

pub use reader::read_flatgeobuf;
pub use writer::{write_flatgeobuf, write_flatgeobuf_with_options};
