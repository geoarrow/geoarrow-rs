//! Defines [`GeoArrowError`], representing all errors returned by this crate.

use arrow_schema::ArrowError;
use std::borrow::Cow;
use std::fmt::Debug;
use thiserror::Error;

/// Enum with all errors in this crate.
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum GeoArrowError {
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

    /// [geo::vincenty_distance::FailedToConvergeError]
    #[error(transparent)]
    FailedToConvergeError(#[from] geo::vincenty_distance::FailedToConvergeError),

    /// [gdal::errors::GdalError]
    #[cfg(feature = "gdal")]
    #[error(transparent)]
    GdalError(#[from] gdal::errors::GdalError),

    /// [geozero::error::GeozeroError]
    #[error(transparent)]
    GeozeroError(#[from] geozero::error::GeozeroError),

    /// [geos::Error]
    #[cfg(feature = "geos")]
    #[error(transparent)]
    GeosError(#[from] geos::Error),

    /// [object_store::Error]
    #[cfg(feature = "flatgeobuf_async")]
    #[error(transparent)]
    ObjectStoreError(#[from] object_store::Error),

    /// [parquet::errors::ParquetError]
    #[cfg(feature = "parquet")]
    #[error(transparent)]
    ParquetError(#[from] parquet::errors::ParquetError),

    /// [polylabel::errors::PolylabelError]
    #[cfg(feature = "polylabel")]
    #[error(transparent)]
    PolylabelError(#[from] polylabel::errors::PolylabelError),

    /// [proj::ProjError]
    #[cfg(feature = "proj")]
    #[error(transparent)]
    ProjError(#[from] proj::ProjError),

    /// [flatgeobuf::Error]
    #[cfg(feature = "flatgeobuf")]
    #[error(transparent)]
    FlatgeobufError(#[from] flatgeobuf::Error),

    /// [std::io::Error]
    #[error(transparent)]
    IOError(#[from] std::io::Error),

    /// [serde_json::Error]
    #[error(transparent)]
    SerdeJsonError(#[from] serde_json::Error),

    /// [sqlx::Error]
    #[cfg(feature = "postgis")]
    #[error(transparent)]
    SqlxError(#[from] sqlx::Error),

    /// [wkb::error::WKBError]
    #[error(transparent)]
    WkbError(#[from] wkb::error::WKBError),

    /// [wkt::error::Error]
    #[error("WKT error: {0}")]
    WktStrError(&'static str),

    /// [wkt::error::Error]
    #[error(transparent)]
    WktError(#[from] wkt::error::Error),
}

/// Crate-specific result type.
pub type Result<T> = std::result::Result<T, GeoArrowError>;

impl From<GeoArrowError> for ArrowError {
    fn from(err: GeoArrowError) -> Self {
        match err {
            GeoArrowError::Arrow(err) => err,
            _ => ArrowError::ExternalError(Box::new(err)),
        }
    }
}
