use geoarrow_schema::Dimension;
use wasm_bindgen::prelude::*;

#[wasm_bindgen(js_name = Dimension)]
pub enum JsDimension {
    XY,
    XYZ,
    XYM,
    XYZM,
}

impl From<JsDimension> for Dimension {
    fn from(value: JsDimension) -> Self {
        match value {
            JsDimension::XY => Dimension::XY,
            JsDimension::XYZ => Dimension::XYZ,
            JsDimension::XYM => Dimension::XYM,
            JsDimension::XYZM => Dimension::XYZM,
        }
    }
}

impl From<Dimension> for JsDimension {
    fn from(value: Dimension) -> Self {
        match value {
            Dimension::XY => JsDimension::XY,
            Dimension::XYZ => JsDimension::XYZ,
            Dimension::XYM => JsDimension::XYM,
            Dimension::XYZM => JsDimension::XYZM,
        }
    }
}
