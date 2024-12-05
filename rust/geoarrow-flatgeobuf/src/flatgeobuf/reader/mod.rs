#[cfg(feature = "async")]
mod r#async;
mod common;
#[cfg(feature = "async")]
mod object_store_reader;
mod sync;

pub use common::FlatGeobufReaderOptions;
#[cfg(feature = "async")]
pub use r#async::read_flatgeobuf_async;
pub use sync::read_flatgeobuf;
