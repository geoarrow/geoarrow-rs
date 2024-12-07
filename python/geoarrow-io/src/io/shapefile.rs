use crate::error::PyGeoArrowResult;
use crate::io::input::sync::FileReader;
use crate::util::to_arro3_table;
use geoarrow::io::shapefile::read_shapefile as _read_shapefile;
use pyo3::prelude::*;
use pyo3_arrow::export::Arro3Table;

#[pyfunction]
// #[pyo3(signature = (file, *, batch_size=65536))]
pub fn read_shapefile(
    mut shp_file: FileReader,
    mut dbf_file: FileReader,
) -> PyGeoArrowResult<Arro3Table> {
    let table = _read_shapefile(&mut shp_file, &mut dbf_file)?;
    Ok(to_arro3_table(table))
}
