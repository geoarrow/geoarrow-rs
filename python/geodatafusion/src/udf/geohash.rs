use geodatafusion::udf::geohash::{Box2DFromGeoHash, GeoHash, PointFromGeoHash};
use pyo3::prelude::*;

use crate::{impl_udf, impl_udf_coord_type_arg};

impl_udf!(Box2DFromGeoHash, PyBox2DFromGeoHash, "Box2DFromGeoHash");
impl_udf!(GeoHash, PyGeoHash, "GeoHash");

impl_udf_coord_type_arg!(PointFromGeoHash, PyPointFromGeoHash, "PointFromGeoHash");

#[pymodule]
pub(crate) fn geohash(m: &Bound<PyModule>) -> PyResult<()> {
    m.add_class::<PyGeoHash>()?;
    m.add_class::<PyBox2DFromGeoHash>()?;
    m.add_class::<PyPointFromGeoHash>()?;

    Ok(())
}
