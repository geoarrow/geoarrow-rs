mod coord_dim;
mod envelope;
mod line_string;

use datafusion::prelude::SessionContext;

/// Register all provided [geo] functions for constructing geometries
pub fn register_udfs(ctx: &SessionContext) {
    ctx.register_udf(coord_dim::CoordDim::new().into());
    ctx.register_udf(envelope::Envelope::new().into());
    ctx.register_udf(line_string::StartPoint::new().into());
}
