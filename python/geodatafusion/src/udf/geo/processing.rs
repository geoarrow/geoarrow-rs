use geodatafusion::udf::geo::processing::{
    Centroid, ConvexHull, OrientedEnvelope, PointOnSurface, Simplify, SimplifyPreserveTopology,
    SimplifyVW,
};

use crate::impl_udf_coord_type_arg;

impl_udf_coord_type_arg!(Centroid, PyCentroid, "Centroid");
impl_udf_coord_type_arg!(ConvexHull, PyConvexHull, "ConvexHull");
impl_udf_coord_type_arg!(OrientedEnvelope, PyOrientedEnvelope, "OrientedEnvelope");
impl_udf_coord_type_arg!(PointOnSurface, PyPointOnSurface, "PointOnSurface");
impl_udf_coord_type_arg!(Simplify, PySimplify, "Simplify");
impl_udf_coord_type_arg!(
    SimplifyPreserveTopology,
    PySimplifyPreserveTopology,
    "SimplifyPreserveTopology"
);
impl_udf_coord_type_arg!(SimplifyVW, PySimplifyVW, "SimplifyVW");
