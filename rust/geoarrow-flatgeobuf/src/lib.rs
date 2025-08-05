//! Read from and write to [FlatGeobuf](https://flatgeobuf.org/) files.
//!
//! For more information, refer to module documentation for [`reader`].

#![warn(missing_docs)]
#![cfg_attr(not(test), deny(unused_crate_dependencies))]

pub mod reader;
pub mod writer;
