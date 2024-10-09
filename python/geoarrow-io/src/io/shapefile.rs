use crate::error::PyGeoArrowResult;
use crate::io::input::sync::FileReader;
use crate::util::table_to_pytable;
use geoarrow::io::shapefile::read_shapefile as _read_shapefile;
use pyo3::prelude::*;

#[pyfunction]
// #[pyo3(signature = (file, *, batch_size=65536))]
pub fn read_shapefile(
    py: Python,
    mut shp_file: FileReader,
    mut dbf_file: FileReader,
) -> PyGeoArrowResult<PyObject> {
    let table = _read_shapefile(&mut shp_file, &mut dbf_file)?;
    Ok(table_to_pytable(table).to_arro3(py)?)
}
