#[cfg(feature = "async")]
mod r#async;
mod sync;

#[cfg(feature = "async")]
pub use r#async::read_flatgeobuf_async;
pub use sync::{read_flatgeobuf, write_flatgeobuf};
