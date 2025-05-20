use geoarrow_array::GeoArrowType;
use geoarrow_array::builder::WkbBuilder;
use geoarrow_cast::cast::cast;
use geoarrow_cast::downcast::{NativeType, infer_downcast_type};
use geoarrow_schema::{CoordType, Dimension, PointType};
use wkt::wkt;

fn main() {
    let mut builder = WkbBuilder::<i32>::new(Default::default());

    builder.push_geometry(Some(&wkt!(POINT (0. 1.))));
    builder.push_geometry(Some(&wkt!(POINT (2. 3.))));
    builder.push_geometry(Some(&wkt!(POINT (4. 5.))));

    let wkb_array = builder.finish();

    let (native_type, dim) = infer_downcast_type(std::iter::once(&wkb_array as _))
        .unwrap()
        .unwrap();
    assert_eq!(native_type, NativeType::Point);
    assert_eq!(dim, Dimension::XY);

    let point_type = PointType::new(CoordType::Separated, Dimension::XY, Default::default());
    cast(&wkb_array, &GeoArrowType::Point(point_type)).unwrap();
}
