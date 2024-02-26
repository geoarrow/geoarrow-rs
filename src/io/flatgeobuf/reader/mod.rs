#[cfg(feature = "flatgeobuf_async")]
mod r#async;
mod common;
#[cfg(feature = "flatgeobuf_async")]
mod object_store_reader;
mod sync;

#[cfg(feature = "flatgeobuf_async")]
pub use r#async::read_flatgeobuf_async;
pub use sync::read_flatgeobuf;
