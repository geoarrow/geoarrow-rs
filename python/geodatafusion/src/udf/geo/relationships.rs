use geodatafusion::udf::geo::relationships::{
    Contains, CoveredBy, Covers, Disjoint, Intersects, Overlaps, Touches,
};

use crate::impl_udf;

impl_udf!(Contains, PyContains, "Contains");
impl_udf!(CoveredBy, PyCoveredBy, "CoveredBy");
impl_udf!(Covers, PyCovers, "Covers");
impl_udf!(Disjoint, PyDisjoint, "Disjoint");
impl_udf!(Intersects, PyIntersects, "Intersects");
impl_udf!(Overlaps, PyOverlaps, "Overlaps");
impl_udf!(Touches, PyTouches, "Touches");
