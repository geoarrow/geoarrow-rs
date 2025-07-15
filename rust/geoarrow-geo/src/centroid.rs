use geo::Centroid;
use geo_traits::to_geo::ToGeoGeometry;
use geoarrow_array::array::PointArray;
use geoarrow_array::builder::PointBuilder;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_schema::error::{GeoArrowError, GeoArrowResult};
use geoarrow_schema::{CoordType, Dimension, PointType};

pub fn centroid(array: &dyn GeoArrowArray, coord_type: CoordType) -> GeoArrowResult<PointArray> {
    downcast_geoarrow_array!(array, _centroid_impl, coord_type)
}

fn _centroid_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    coord_type: CoordType,
) -> GeoArrowResult<PointArray> {
    let typ = PointType::new(Dimension::XY, array.data_type().metadata().clone())
        .with_coord_type(coord_type);
    let mut builder = PointBuilder::with_capacity(typ, array.len());

    for item in array.iter() {
        if let Some(geom) = item {
            let geo_geom = geom?
                .try_to_geometry()
                .ok_or(GeoArrowError::IncorrectGeometryType(
                    "geo crate does not support empty points.".to_string(),
                ))?;
            let centroid = geo_geom.centroid();
            builder.push_point(centroid.as_ref());
        } else {
            builder.push_null();
        }
    }

    Ok(builder.finish())
}
