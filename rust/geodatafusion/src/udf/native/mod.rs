//! User-defined functions that wrap native Rust implementations.

mod accessors;
mod bounding_box;
mod constructors;
mod io;
mod measurement;
mod processing;

use datafusion::prelude::SessionContext;

/// Register all provided native-Rust functions
pub fn register_native(ctx: &SessionContext) {
    accessors::register_udfs(ctx);
    bounding_box::register_udfs(ctx);
    constructors::register_udfs(ctx);
    io::register_udfs(ctx);
    measurement::register_udfs(ctx);
    processing::register_udfs(ctx);
}
