use std::fmt::Debug;

use serde_json::Value;

use crate::array::metadata::ArrayMetadata;
use crate::error::{GeoArrowError, Result};

/// CRS transforms used for writing GeoArrow data to file formats that require different CRS
/// representations.
pub trait CRSTransform: Debug {
    /// Convert the CRS contained in this ArrayMetadata to a PROJJSON object.
    ///
    /// Users should prefer calling `extract_projjson`, which will first unwrap the underlying
    /// array metadata if it's already PROJJSON.
    fn _convert_to_projjson(&self, meta: &ArrayMetadata) -> Result<Option<Value>>;

    /// Convert the CRS contained in this ArrayMetadata to a WKT string.
    ///
    /// Users should prefer calling `extract_wkt`, which will first unwrap the underlying
    /// array metadata if it's already PROJJSON.
    fn _convert_to_wkt(&self, meta: &ArrayMetadata) -> Result<Option<String>>;

    /// Extract PROJJSON from the provided metadata.
    ///
    /// If the CRS is already stored as PROJJSON, this will return that. Otherwise it will call
    /// [`Self::_convert_to_projjson`].
    fn extract_projjson(&self, meta: &ArrayMetadata) -> Result<Option<Value>> {
        if let Some(crs) = &meta.crs {
            if matches!(crs, Value::Object(_)) {
                return Ok::<_, GeoArrowError>(Some(crs.clone()));
            }
        }

        self._convert_to_projjson(meta)
    }

    /// Extract WKT from the provided metadata.
    ///
    /// If the CRS is already stored as WKT, this will return that. Otherwise it will call
    /// [`Self::_convert_to_wkt`].
    fn extract_wkt(&self, meta: &ArrayMetadata) -> Result<Option<String>> {
        if let (Some(crs), Some(crs_type)) = (&meta.crs, &meta.crs_type) {
            if crs_type == "wkt2:2019" {
                if let Value::String(inner) = crs {
                    return Ok::<_, GeoArrowError>(Some(inner.clone()));
                }
            }
        }

        self._convert_to_wkt(meta)
    }
}

/// A default implementation for [CRSTransform] which errors on any CRS conversion.
#[derive(Debug, Clone, Default)]
pub struct DefaultCRSTransform {}

impl CRSTransform for DefaultCRSTransform {
    fn _convert_to_projjson(&self, _meta: &ArrayMetadata) -> Result<Option<Value>> {
        Err(GeoArrowError::General(
            "Unable to convert CRS to PROJJSON".to_string(),
        ))
    }

    fn _convert_to_wkt(&self, _meta: &ArrayMetadata) -> Result<Option<String>> {
        Err(GeoArrowError::General(
            "Unable to convert CRS to WKT".to_string(),
        ))
    }
}
