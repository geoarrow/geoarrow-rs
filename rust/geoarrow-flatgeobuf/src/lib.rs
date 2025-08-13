//! Read from and write to [FlatGeobuf](https://flatgeobuf.org/) files.
//!
//! For more information, refer to module documentation for [`reader`].

#![warn(missing_docs)]
#![cfg_attr(docsrs, feature(doc_auto_cfg))]
#![cfg_attr(not(test), deny(unused_crate_dependencies))]
#![doc(
    html_logo_url = "https://github.com/geoarrow.png",
    html_favicon_url = "https://github.com/geoarrow.png?size=32"
)]

pub mod reader;
pub mod writer;
