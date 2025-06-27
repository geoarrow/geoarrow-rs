use std::sync::Arc;

use geo::Simplify;
use geo_traits::to_geo::{ToGeoGeometry, ToGeoLineString};
use geoarrow_array::builder::{GeometryBuilder, LineStringBuilder};
use geoarrow_array::cast::AsGeoArrowArray;
use geoarrow_array::scalar::LineString;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};
use geoarrow_schema::error::GeoArrowResult;
use geoarrow_schema::{GeoArrowType, GeometryType};

pub fn simplify<'a>(
    array: &'a (impl GeoArrowArrayAccessor<'a> + AsGeoArrowArray),
    epsilon: &f64,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    match array.data_type() {
        GeoArrowType::LineString(typ) => {
            let mut builder = LineStringBuilder::new(typ);
            for item in array.as_line_string().iter() {
                if let Some(geom) = item {
                    let simplified_geom = &geom?.to_line_string().simplify(epsilon);
                    builder.push_line_string(Some(simplified_geom))?;
                } else {
                    builder.push_line_string(None::<&LineString>)?;
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        GeoArrowType::Wkb(typ) => {
            let geom_typ = GeometryType::new(typ.metadata().clone());
            let mut builder = GeometryBuilder::new(geom_typ);
            for item in array.as_line_string().iter() {
                if let Some(geom) = item {
                    let simplified_geom = simplify_geometry(&geom?.to_geometry(), epsilon);
                    builder.push_geometry(Some(&simplified_geom))?;
                } else {
                    builder.push_geometry(None::<&LineString>)?;
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        _ => todo!(),
    }
}

fn simplify_geometry(geom: &geo::Geometry, epsilon: &f64) -> geo::Geometry {
    match geom {
        geo::Geometry::LineString(g) => geo::Geometry::LineString(g.simplify(epsilon)),
        geo::Geometry::Polygon(g) => geo::Geometry::Polygon(g.simplify(epsilon)),
        geo::Geometry::MultiLineString(g) => geo::Geometry::MultiLineString(g.simplify(epsilon)),
        geo::Geometry::MultiPolygon(g) => geo::Geometry::MultiPolygon(g.simplify(epsilon)),
        _ => panic!("Unsupported geometry type for simplification: {geom:?}"),
    }
}
