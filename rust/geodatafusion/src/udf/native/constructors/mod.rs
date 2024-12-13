mod point;

use datafusion::prelude::SessionContext;

/// Register all provided [geo] functions for constructing geometries
pub fn register_udfs(ctx: &SessionContext) {
    ctx.register_udf(point::Point::new().into());
    ctx.register_udf(point::MakePoint::new().into());
}
