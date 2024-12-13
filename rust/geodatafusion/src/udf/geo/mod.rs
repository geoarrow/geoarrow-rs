//! User-defined functions that wrap the [geo] crate.

mod accessors;
mod bounding_box;
mod constructors;
mod coord_dim;
mod linear_ref;
mod measurement;
mod processing;

pub use coord_dim::coord_dim;

use datafusion::prelude::SessionContext;

/// Register all provided [geo] functions
pub fn register_geo(ctx: &SessionContext) {
    constructors::register_constructors(ctx);
    measurement::register_measurement(ctx);

    ctx.register_udf(coord_dim());
}
