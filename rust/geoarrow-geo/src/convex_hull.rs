use geo::ConvexHull;
use geoarrow_array::array::PolygonArray;
use geoarrow_array::builder::PolygonBuilder;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_schema::error::GeoArrowResult;
use geoarrow_schema::{Dimension, PolygonType};

use crate::util::to_geo::geometry_to_geo;

pub fn convex_hull(array: &dyn GeoArrowArray) -> GeoArrowResult<PolygonArray> {
    downcast_geoarrow_array!(array, convex_hull_impl)
}

fn convex_hull_impl<'a>(array: &'a impl GeoArrowArrayAccessor<'a>) -> GeoArrowResult<PolygonArray> {
    let coord_type = array.data_type().coord_type().unwrap_or_default();
    let typ = PolygonType::new(Dimension::XY, array.data_type().metadata().clone())
        .with_coord_type(coord_type);
    let mut builder = PolygonBuilder::new(typ);

    for item in array.iter() {
        if let Some(geom) = item {
            let geo_geom = geometry_to_geo(&geom?)?;
            let poly = geo_geom.convex_hull();
            builder.push_polygon(Some(&poly))?;
        } else {
            builder.push_polygon(None::<geo::Polygon>.as_ref())?;
        }
    }

    Ok(builder.finish())
}
