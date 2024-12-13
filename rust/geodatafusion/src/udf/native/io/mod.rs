//! Geometry Input and Output

mod wkb;
mod wkt;

use datafusion::prelude::SessionContext;

/// Register all provided functions for geometry input and output
pub fn register_udfs(ctx: &SessionContext) {
    ctx.register_udf(wkb::AsBinary::new().into());
    ctx.register_udf(wkb::GeomFromWKB::new().into());
    ctx.register_udf(wkt::AsText::new().into());
    ctx.register_udf(wkt::GeomFromText::new().into());
}
