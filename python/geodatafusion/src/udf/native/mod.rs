mod accessors;
mod bounding_box;
mod constructors;
mod io;

use pyo3::prelude::*;

#[pymodule]
pub(crate) fn native(m: &Bound<PyModule>) -> PyResult<()> {
    // accessors
    m.add_class::<accessors::PyCoordDim>()?;
    m.add_class::<accessors::PyNDims>()?;
    m.add_class::<accessors::PyX>()?;
    m.add_class::<accessors::PyY>()?;
    m.add_class::<accessors::PyZ>()?;
    m.add_class::<accessors::PyM>()?;

    // bounding_box
    m.add_class::<bounding_box::PyBox2D>()?;
    m.add_class::<bounding_box::PyBox3D>()?;
    m.add_class::<bounding_box::PyXMin>()?;
    m.add_class::<bounding_box::PyXMax>()?;
    m.add_class::<bounding_box::PyYMin>()?;
    m.add_class::<bounding_box::PyYMax>()?;
    m.add_class::<bounding_box::PyZMin>()?;
    m.add_class::<bounding_box::PyZMax>()?;
    m.add_class::<bounding_box::PyMakeBox2D>()?;
    m.add_class::<bounding_box::PyMakeBox3D>()?;

    // constructors
    m.add_class::<constructors::PyPoint>()?;
    m.add_class::<constructors::PyPointZ>()?;
    m.add_class::<constructors::PyPointM>()?;
    m.add_class::<constructors::PyPointZM>()?;
    m.add_class::<constructors::PyMakePoint>()?;
    m.add_class::<constructors::PyMakePointM>()?;

    // io
    m.add_class::<io::PyAsBinary>()?;
    m.add_class::<io::PyAsText>()?;
    m.add_class::<io::PyGeomFromText>()?;
    m.add_class::<io::PyGeomFromWKB>()?;

    Ok(())
}
