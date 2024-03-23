#[cfg(feature = "io_flatgeobuf")]
pub mod flatgeobuf;
#[cfg(feature = "io_geojson")]
pub mod geojson;
// #[cfg(feature = "io_object_store")]
// pub mod object_store;
// #[cfg(feature = "io_object_store")]
// pub mod object_store_s3;
#[cfg(feature = "io_parquet")]
pub mod parquet;
