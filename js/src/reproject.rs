use geoarrow::algorithm::geodesy::Direction;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub enum ReprojectDirection {
    /// `Fwd`: Indicate that a two-way operator, function, or method,
    /// should run in the *forward* direction.
    Fwd,

    /// `Inv`: Indicate that a two-way operator, function, or method,
    /// should run in the *inverse* direction.
    Inv,
}

impl From<ReprojectDirection> for Direction {
    fn from(value: ReprojectDirection) -> Self {
        match value {
            ReprojectDirection::Fwd => Self::Fwd,
            ReprojectDirection::Inv => Self::Inv,
        }
    }
}

impl From<Direction> for ReprojectDirection {
    fn from(value: Direction) -> Self {
        match value {
            Direction::Fwd => Self::Fwd,
            Direction::Inv => Self::Inv,
        }
    }
}
