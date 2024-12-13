mod box_2d;
mod expand;
mod extrema;
mod make_box_2d;

use datafusion::prelude::SessionContext;

/// Register all provided bounding box functions
pub fn register_udfs(ctx: &SessionContext) {
    ctx.register_udf(box_2d::Box2D::new().into());
    ctx.register_udf(expand::Expand::new().into());
    ctx.register_udf(extrema::XMax::new().into());
    ctx.register_udf(extrema::XMin::new().into());
    ctx.register_udf(extrema::YMax::new().into());
    ctx.register_udf(extrema::YMin::new().into());
    ctx.register_udf(make_box_2d::MakeBox2D::new().into());
}
