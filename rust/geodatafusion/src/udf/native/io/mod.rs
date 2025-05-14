//! Geometry Input and Output

// mod geohash;
mod wkb;
mod wkt;

pub use wkb::{AsBinary, GeomFromWKB};
pub use wkt::{AsText, GeomFromText};

// use datafusion::prelude::SessionContext;

// /// Register all provided functions for geometry input and output
// pub fn register_udfs(ctx: &SessionContext) {
//     ctx.register_udf(geohash::Box2DFromGeoHash::new().into());
//     ctx.register_udf(geohash::GeoHash::new().into());
//     ctx.register_udf(geohash::PointFromGeoHash::new().into());
//     ctx.register_udf(wkb::AsBinary::new().into());
//     ctx.register_udf(wkb::GeomFromWKB::new().into());
//     ctx.register_udf(wkt::AsText::new().into());
//     ctx.register_udf(wkt::GeomFromText::new().into());
// }
