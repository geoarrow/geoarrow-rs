//! Defines [`GeoArrowError`], representing all errors returned by this crate.

use arrow_schema::ArrowError;
use std::borrow::Cow;
use std::error::Error;
use std::fmt::Debug;
use thiserror::Error;

/// Enum with all errors in this crate.
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum GeoArrowError {
    /// Wraps an external error.
    #[error("External error: {0}")]
    External(#[from] Box<dyn Error + Send + Sync>),

    /// Incorrect type was passed to an operation.
    #[error("Incorrect type passed to operation: {0}")]
    IncorrectType(Cow<'static, str>),

    /// Returned when functionality is not yet available.
    #[error("Not yet implemented: {0}")]
    NotYetImplemented(String),

    /// General error.
    #[error("General error: {0}")]
    General(String),

    /// Whenever pushing to a container fails because it does not support more entries.
    ///
    /// The solution is usually to use a higher-capacity container-backing type.
    #[error("Overflow")]
    Overflow,

    /// [ArrowError]
    #[error(transparent)]
    Arrow(#[from] ArrowError),

    /// Error during casting from one type to another
    #[error("Error during casting from one type to another: {0}")]
    Cast(String),

    /// [std::io::Error]
    #[error(transparent)]
    IOError(#[from] std::io::Error),

    /// [serde_json::Error]
    #[error(transparent)]
    SerdeJsonError(#[from] serde_json::Error),

    /// [wkt::error::Error]
    #[error("WKT error: {0}")]
    WktStrError(&'static str),
}

/// Crate-specific result type.
pub type GeoArrowResult<T> = std::result::Result<T, GeoArrowError>;

impl From<GeoArrowError> for ArrowError {
    /// Many APIs where we pass in a callback into the Arrow crate require the returned error type
    /// to be ArrowError, so implementing this `From` makes the conversion less verbose there.
    fn from(err: GeoArrowError) -> Self {
        match err {
            GeoArrowError::Arrow(err) => err,
            _ => ArrowError::ExternalError(Box::new(err)),
        }
    }
}
