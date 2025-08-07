#![doc = include_str!("../README.md")]
#![warn(missing_docs)]
#![cfg_attr(docsrs, feature(doc_auto_cfg))]
#![cfg_attr(not(test), deny(unused_crate_dependencies))]
#![doc(
    html_logo_url = "https://github.com/geoarrow.png",
    html_favicon_url = "https://github.com/geoarrow.png?size=32"
)]

pub mod array;
pub mod builder;
pub mod capacity;
pub mod cast;
mod eq;
#[cfg(feature = "geozero")]
pub mod geozero;
pub mod scalar;
mod trait_;
pub(crate) mod util;

pub use trait_::{
    GeoArrowArray, GeoArrowArrayAccessor, GeoArrowArrayIterator, GeoArrowArrayReader, IntoArrow,
};

#[cfg(any(test, feature = "test-data"))]
#[allow(missing_docs)]
pub mod test;
