mod area;

use datafusion::prelude::SessionContext;

/// Register all provided [geo] functions for constructing geometries
pub fn register_measurement(ctx: &SessionContext) {
    ctx.register_udf(area::Area::new().into());
}
