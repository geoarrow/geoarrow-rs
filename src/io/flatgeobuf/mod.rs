//! Read the [FlatGeobuf](https://flatgeobuf.org/) format.

pub mod anyvalue;
pub mod reader;
pub mod writer;

pub use reader::read_flatgeobuf;
pub use writer::{write_flatgeobuf, write_flatgeobuf_with_options};
