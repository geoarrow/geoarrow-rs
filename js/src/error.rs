use arrow2::error::Error as ArrowError;
use thiserror::Error;
use wasm_bindgen::JsError;

#[derive(Error, Debug)]
pub enum GeoArrowError {
    #[error(transparent)]
    ArrowError(Box<ArrowError>),

    #[error("Internal error: `{0}`")]
    InternalError(String),
}

pub type Result<T> = std::result::Result<T, GeoArrowError>;
pub type WasmResult<T> = std::result::Result<T, JsError>;

impl From<ArrowError> for GeoArrowError {
    fn from(err: ArrowError) -> Self {
        Self::ArrowError(Box::new(err))
    }
}
