mod measurement;
mod processing;
mod relationships;
mod validation;

use pyo3::prelude::*;

#[pymodule]
pub(crate) fn geo(m: &Bound<PyModule>) -> PyResult<()> {
    // measurement
    m.add_class::<measurement::PyArea>()?;
    m.add_class::<measurement::PyDistance>()?;
    m.add_class::<measurement::PyLength>()?;

    // processing
    m.add_class::<processing::PyCentroid>()?;
    m.add_class::<processing::PyConvexHull>()?;
    m.add_class::<processing::PyOrientedEnvelope>()?;
    m.add_class::<processing::PyPointOnSurface>()?;
    m.add_class::<processing::PySimplify>()?;
    m.add_class::<processing::PySimplifyPreserveTopology>()?;
    m.add_class::<processing::PySimplifyVW>()?;

    // relationships
    m.add_class::<relationships::PyContains>()?;
    m.add_class::<relationships::PyCoveredBy>()?;
    m.add_class::<relationships::PyCovers>()?;
    m.add_class::<relationships::PyDisjoint>()?;
    m.add_class::<relationships::PyIntersects>()?;
    m.add_class::<relationships::PyOverlaps>()?;
    m.add_class::<relationships::PyTouches>()?;

    // validation
    m.add_class::<validation::PyIsValid>()?;
    m.add_class::<validation::PyIsValidReason>()?;

    Ok(())
}
