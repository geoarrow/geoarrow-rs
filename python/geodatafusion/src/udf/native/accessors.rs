use geodatafusion::udf::native::accessors::{CoordDim, M, NDims, X, Y, Z};

use crate::impl_udf;

impl_udf!(CoordDim, PyCoordDim, "CoordDim");
impl_udf!(NDims, PyNDims, "NDims");
impl_udf!(X, PyX, "X");
impl_udf!(Y, PyY, "Y");
impl_udf!(Z, PyZ, "Z");
impl_udf!(M, PyM, "M");
