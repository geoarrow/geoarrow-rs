mod r#box;
mod extrema;

use datafusion::prelude::SessionContext;

/// Register all provided bounding box functions
pub fn register_udfs(ctx: &SessionContext) {
    ctx.register_udf(extrema::XMin::new().into());
    ctx.register_udf(extrema::YMin::new().into());
    ctx.register_udf(extrema::XMax::new().into());
    ctx.register_udf(extrema::YMax::new().into());
    ctx.register_udf(r#box::Box2D::new().into());
}
