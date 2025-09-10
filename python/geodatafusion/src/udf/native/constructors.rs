use geodatafusion::udf::native::constructors::{
    MakePoint, MakePointM, Point, PointM, PointZ, PointZM,
};

use crate::impl_udf_coord_type_arg;

impl_udf_coord_type_arg!(Point, PyPoint, "Point");
impl_udf_coord_type_arg!(PointZ, PyPointZ, "PointZ");
impl_udf_coord_type_arg!(PointM, PyPointM, "PointM");
impl_udf_coord_type_arg!(PointZM, PyPointZM, "PointZM");
impl_udf_coord_type_arg!(MakePoint, PyMakePoint, "MakePoint");
impl_udf_coord_type_arg!(MakePointM, PyMakePointM, "MakePointM");
