//! Contains an implementation of reading from PostGIS databases.

mod reader;
mod type_info;

pub use reader::read_postgis;
