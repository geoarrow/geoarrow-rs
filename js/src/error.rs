// use arrow2::error::Error as ArrowError;
use arrow_wasm::error::ArrowWasmError;
use thiserror::Error;
use wasm_bindgen::JsError;

#[derive(Error, Debug)]
pub enum GeoArrowWasmError {
    // #[error(transparent)]
    // ArrowError(Box<ArrowError>),
    #[error(transparent)]
    ArrowWasmError(Box<ArrowWasmError>),

    #[cfg(feature = "io_object_store")]
    #[error(transparent)]
    ObjectStoreError(#[from] object_store::Error),

    #[cfg(feature = "io_parquet")]
    #[error(transparent)]
    ParquetError(#[from] parquet::errors::ParquetError),

    #[error("Internal error: `{0}`")]
    InternalError(String),
}

pub type Result<T> = std::result::Result<T, GeoArrowWasmError>;
pub type WasmResult<T> = std::result::Result<T, JsError>;
