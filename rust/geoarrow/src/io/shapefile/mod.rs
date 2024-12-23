//! Read from [Shapefile](https://www.esri.com/content/dam/esrisites/sitecore-archive/Files/Pdfs/library/whitepapers/pdfs/shapefile.pdf) datasets.
//!
//! This wraps the [shapefile] crate.

mod reader;
mod scalar;

pub use reader::{ShapefileReaderBuilder, ShapefileReaderOptions};
