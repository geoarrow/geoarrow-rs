mod envelope;

use datafusion::prelude::SessionContext;

/// Register all provided [geo] functions for constructing geometries
pub fn register_constructors(ctx: &SessionContext) {
    ctx.register_udf(envelope::Envelope::new().into());
}
