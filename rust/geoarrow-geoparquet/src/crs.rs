//! Defines CRS transforms used for writing GeoArrow data to file formats that require different
//! CRS representations.

use std::fmt::Debug;

use geoarrow_array::error::{GeoArrowError, Result};
use geoarrow_schema::{Crs, CrsType};
use serde_json::Value;

/// CRS transforms used for writing GeoArrow data to file formats that require different CRS
/// representations.
pub trait CRSTransform: Debug {
    /// Convert the CRS contained in this Metadata to a PROJJSON object.
    ///
    /// Users should prefer calling `extract_projjson`, which will first unwrap the underlying
    /// array metadata if it's already PROJJSON.
    fn _convert_to_projjson(&self, crs: &Crs) -> Result<Option<Value>>;

    /// Convert the CRS contained in this Metadata to a WKT string.
    ///
    /// Users should prefer calling `extract_wkt`, which will first unwrap the underlying
    /// array metadata if it's already PROJJSON.
    fn _convert_to_wkt(&self, crs: &Crs) -> Result<Option<String>>;

    /// Extract PROJJSON from the provided metadata.
    ///
    /// If the CRS is already stored as PROJJSON, this will return that. Otherwise it will call
    /// [`Self::_convert_to_projjson`].
    fn extract_projjson(&self, crs: &Crs) -> Result<Option<Value>> {
        match crs.crs_type() {
            Some(CrsType::Projjson) => Ok(crs.crs_value().cloned()),
            _ => self._convert_to_projjson(crs),
        }
    }

    /// Extract WKT from the provided metadata.
    ///
    /// If the CRS is already stored as WKT, this will return that. Otherwise it will call
    /// [`Self::_convert_to_wkt`].
    fn extract_wkt(&self, crs: &Crs) -> Result<Option<String>> {
        if let (Some(crs), Some(crs_type)) = (crs.crs_value(), crs.crs_type()) {
            if crs_type == CrsType::Wkt2_2019 {
                if let Value::String(inner) = crs {
                    return Ok::<_, GeoArrowError>(Some(inner.clone()));
                }
            }
        }

        self._convert_to_wkt(crs)
    }
}

/// A default implementation for [CRSTransform] which does not do any CRS conversion.
///
/// Instead of raising an error, this will **silently drop any CRS information when writing data**.
#[derive(Debug, Clone, Default)]
pub struct DefaultCRSTransform {}

impl CRSTransform for DefaultCRSTransform {
    fn _convert_to_projjson(&self, _crs: &Crs) -> Result<Option<Value>> {
        // Unable to convert CRS to PROJJSON
        // So we proceed with missing CRS
        // TODO: we should probably log this.
        Ok(None)
    }

    fn _convert_to_wkt(&self, _crs: &Crs) -> Result<Option<String>> {
        // Unable to convert CRS to WKT
        // So we proceed with missing CRS
        // TODO: we should probably log this.
        Ok(None)
    }
}
