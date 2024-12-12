//! User-defined functions that wrap the [geo] crate.

mod area;
mod centroid;
mod wkb;
mod wkt;

pub use area::area;
pub use centroid::centroid;
pub use wkb::{as_binary, from_wkb};
pub use wkt::{as_text, from_text};

use datafusion::prelude::SessionContext;

/// Register all provided [geo] functions
pub fn register_geo(ctx: SessionContext) {
    ctx.register_udf(area());
    ctx.register_udf(as_binary());
    ctx.register_udf(as_text());
    ctx.register_udf(centroid());
    ctx.register_udf(from_text());
    ctx.register_udf(from_wkb());
}
