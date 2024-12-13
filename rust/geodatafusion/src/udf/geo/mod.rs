//! User-defined functions that wrap the [geo] crate.

mod r#box;
mod centroid;
mod constructors;
mod convex_hull;
mod coord_dim;
mod envelope;
mod linear_ref;
mod measurement;

pub use centroid::centroid;
pub use convex_hull::convex_hull;
pub use coord_dim::coord_dim;
pub use envelope::envelope;
pub use r#box::{box_2d, xmax, xmin, ymax, ymin};

use datafusion::prelude::SessionContext;

/// Register all provided [geo] functions
pub fn register_geo(ctx: &SessionContext) {
    constructors::register_constructors(ctx);
    measurement::register_measurement(ctx);

    ctx.register_udf(centroid());
    ctx.register_udf(convex_hull());
    ctx.register_udf(coord_dim());
    ctx.register_udf(envelope());

    // Box functions
    ctx.register_udf(box_2d());
    ctx.register_udf(xmax());
    ctx.register_udf(xmin());
    ctx.register_udf(ymax());
    ctx.register_udf(ymin());
}
