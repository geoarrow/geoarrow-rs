mod centroid;
mod chaikin_smoothing;
mod convex_hull;

use datafusion::prelude::SessionContext;

/// Register all provided [geo] functions for processing geometries
pub fn register_udfs(ctx: &SessionContext) {
    ctx.register_udf(centroid::Centroid::new().into());
    ctx.register_udf(convex_hull::ConvexHull::new().into());
}
