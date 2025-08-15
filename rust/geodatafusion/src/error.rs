//! Defines [`GeoArrowError`], representing all errors returned by this crate.

use std::fmt::Debug;

use arrow_schema::ArrowError;
use datafusion::error::DataFusionError;
use geoarrow_schema::error::GeoArrowError;
use thiserror::Error;

/// Enum with all errors in this crate.
#[derive(Error, Debug)]
pub(crate) enum GeoDataFusionError {
    #[error(transparent)]
    Arrow(#[from] ArrowError),

    #[error(transparent)]
    DataFusion(#[from] DataFusionError),

    #[error(transparent)]
    GeoArrow(#[from] GeoArrowError),

    #[error(transparent)]
    GeoHash(#[from] geohash::GeohashError),
}

/// Crate-specific result type.
pub(crate) type GeoDataFusionResult<T> = std::result::Result<T, GeoDataFusionError>;

impl From<GeoDataFusionError> for DataFusionError {
    fn from(value: GeoDataFusionError) -> Self {
        match value {
            GeoDataFusionError::Arrow(err) => DataFusionError::ArrowError(Box::new(err), None),
            GeoDataFusionError::DataFusion(err) => err,
            GeoDataFusionError::GeoArrow(err) => DataFusionError::External(Box::new(err)),
            GeoDataFusionError::GeoHash(err) => DataFusionError::External(Box::new(err)),
        }
    }
}
