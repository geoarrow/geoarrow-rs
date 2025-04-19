use geo::ConvexHull;
use geo_traits::to_geo::ToGeoGeometry;
use geoarrow_array::array::PolygonArray;
use geoarrow_array::builder::PolygonBuilder;
use geoarrow_array::error::Result;
use geoarrow_array::{ArrayAccessor, GeoArrowArray, downcast_geoarrow_array};
use geoarrow_schema::{CoordType, Dimension, PolygonType};

pub fn convex_hull(array: &dyn GeoArrowArray) -> Result<PolygonArray> {
    downcast_geoarrow_array!(array, convex_hull_impl)
}

fn convex_hull_impl<'a>(array: &'a impl ArrayAccessor<'a>) -> Result<PolygonArray> {
    let coord_type = array
        .data_type()
        .coord_type()
        .unwrap_or(CoordType::Separated);
    let typ = PolygonType::new(
        coord_type,
        Dimension::XY,
        array.data_type().metadata().clone(),
    );
    let mut builder = PolygonBuilder::new(typ);

    for item in array.iter() {
        if let Some(geom) = item {
            let geo_geom = geom?.to_geometry();
            let poly = geo_geom.convex_hull();
            builder.push_polygon(Some(&poly))?;
        } else {
            builder.push_polygon(None::<geo::Polygon>.as_ref())?;
        }
    }

    Ok(builder.finish())
}
