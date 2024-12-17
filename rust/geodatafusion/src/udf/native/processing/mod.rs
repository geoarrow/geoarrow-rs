mod centroid;
mod chaikin_smoothing;
mod convex_hull;
mod simplify;
mod simplify_preserve_topology;
mod simplify_vw;

use datafusion::prelude::SessionContext;

/// Register all provided [geo] functions for processing geometries
pub fn register_udfs(ctx: &SessionContext) {
    ctx.register_udf(centroid::Centroid::new().into());
    ctx.register_udf(convex_hull::ConvexHull::new().into());
    ctx.register_udf(simplify_preserve_topology::SimplifyPreserveTopology::new().into());
    ctx.register_udf(simplify_vw::SimplifyVw::new().into());
    ctx.register_udf(simplify::Simplify::new().into());
}
