use geodatafusion::udf::native::bounding_box::{
    Box2D, Box3D, MakeBox2D, MakeBox3D, XMax, XMin, YMax, YMin, ZMax, ZMin,
};

use crate::impl_udf;

impl_udf!(Box2D, PyBox2D, "Box2D");
impl_udf!(Box3D, PyBox3D, "Box3D");
impl_udf!(XMin, PyXMin, "XMin");
impl_udf!(XMax, PyXMax, "XMax");
impl_udf!(YMin, PyYMin, "YMin");
impl_udf!(YMax, PyYMax, "YMax");
impl_udf!(ZMin, PyZMin, "ZMin");
impl_udf!(ZMax, PyZMax, "ZMax");
impl_udf!(MakeBox2D, PyMakeBox2D, "MakeBox2D");
impl_udf!(MakeBox3D, PyMakeBox3D, "MakeBox3D");
