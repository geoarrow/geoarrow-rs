//! Defines [`GeoArrowError`], representing all errors returned by this crate.

use arrow_schema::ArrowError;
use std::fmt::Debug;
use thiserror::Error;

/// Enum with all errors in this crate.
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum GeoArrowError {
    /// Returned when functionality is not yet available.
    #[error("Not yet implemented: {0}")]
    NotYetImplemented(String),

    #[error("General error: {0}")]
    General(String),

    /// Wrapper for an error triggered by a dependency
    #[error(transparent)]
    External(#[from] anyhow::Error),

    /// Whenever pushing to a container fails because it does not support more entries.
    /// The solution is usually to use a higher-capacity container-backing type.
    #[error("Overflow")]
    Overflow,

    #[error(transparent)]
    Arrow(#[from] ArrowError),

    #[error(transparent)]
    FailedToConvergeError(#[from] geo::vincenty_distance::FailedToConvergeError),

    #[cfg(feature = "geozero")]
    #[error(transparent)]
    GeozeroError(#[from] geozero::error::GeozeroError),

    #[cfg(feature = "geos")]
    #[error(transparent)]
    GeosError(#[from] geos::Error),

    #[cfg(feature = "proj")]
    #[error(transparent)]
    ProjError(#[from] proj::ProjError),

    #[cfg(feature = "flatgeobuf")]
    #[error(transparent)]
    FlatgeobufError(#[from] flatgeobuf::Error),
}

pub type Result<T> = std::result::Result<T, GeoArrowError>;
