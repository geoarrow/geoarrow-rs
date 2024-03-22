//! The code in this mod was originally vendored from object-store-python under the Apache 2
//! license.
//!
//! https://github.com/roeap/object-store-python/commit/445e9d7fa238fc3cd31cc2820caee0d8e10fedb8

pub mod builder;
pub mod store;

pub use builder::ObjectStoreBuilder;
pub use store::PyObjectStore;
