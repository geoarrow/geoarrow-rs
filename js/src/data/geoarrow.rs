use std::sync::Arc;

use arrow_array::builder::BinaryBuilder;
use arrow_array::{Array, ArrayRef};
use arrow_select::concat::concat;
use arrow_wasm::Table;
use arrow_wasm::ffi::FFIData;
use geoarrow_array::GeoArrowArray;
use geoarrow_array::array::{GenericWkbArray, from_arrow_array};
use geoarrow_array::cast::from_wkb;
use geoarrow_schema::{GeoArrowType, GeometryType, Metadata};
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::js_sys;

use crate::error::WasmResult;

#[wasm_bindgen(js_name = GeoArrowData)]
pub struct JsGeoArrowData(Arc<dyn GeoArrowArray>);

impl JsGeoArrowData {
    pub(crate) fn inner(&self) -> &Arc<dyn GeoArrowArray> {
        &self.0
    }

    pub(crate) fn from_arc(arr: Arc<dyn GeoArrowArray>) -> Self {
        Self(arr)
    }
}

#[wasm_bindgen(js_class = GeoArrowData)]
impl JsGeoArrowData {
    /// The number of rows, null rows included
    #[wasm_bindgen(js_name = numRows)]
    pub fn num_rows_js(&self) -> usize {
        self.0.len()
    }

    #[wasm_bindgen(js_name = numNulls)]
    pub fn num_nulls_js(&self) -> usize {
        self.0.logical_null_count()
    }

    /// The coordinate dimension: `"XY"`, `"XYZ"`, `"XYM"` or `"XYZM"`
    ///
    /// A geometry, WKB or WKT array can hold a different dimension in each row
    /// and gives `"Mixed"`
    #[wasm_bindgen(js_name = dimension)]
    pub fn dimension_js(&self) -> String {
        use geoarrow_schema::Dimension;
        match self.0.data_type().dimension() {
            Some(Dimension::XY) => "XY".to_string(),
            Some(Dimension::XYZ) => "XYZ".to_string(),
            Some(Dimension::XYM) => "XYM".to_string(),
            Some(Dimension::XYZM) => "XYZM".to_string(),
            None => "Mixed".to_string(),
        }
    }

    /// Export this array over the Arrow C data interface, with its GeoArrow
    /// extension metadata; read the result with `parseData` and `parseField` of
    /// [arrow-js-ffi](https://github.com/kylebarron/arrow-js-ffi)
    #[wasm_bindgen(js_name = toFFI)]
    pub fn to_ffi_js(&self) -> WasmResult<FFIData> {
        let field = self.0.data_type().to_field("geometry", true);
        Ok(FFIData::from_arrow(self.0.to_array_ref().as_ref(), &field)?)
    }

    /// Build a GeoArrowData from one WKB blob per row, typed `geoarrow.geometry`
    #[wasm_bindgen(js_name = fromWkb)]
    pub fn from_wkb_js(wkb_blobs: Vec<js_sys::Uint8Array>) -> WasmResult<JsGeoArrowData> {
        let mut builder = BinaryBuilder::new();
        for arr in &wkb_blobs {
            builder.append_value(arr.to_vec());
        }
        let binary = builder.finish();
        let wkb_array = GenericWkbArray::<i32>::new(binary, Arc::new(Metadata::default()));
        let target = GeoArrowType::Geometry(GeometryType::new(Arc::new(Metadata::default())));
        let native = from_wkb(&wkb_array, target)?;
        Ok(Self(native))
    }

    /// Read one column of a table as one GeoArrowData, consuming the table
    ///
    /// A column with no GeoArrow extension metadata is read from its
    /// storage type: binary as WKB, string as WKT, a pair of f64 as a coordinate
    #[wasm_bindgen(js_name = fromTable)]
    pub fn from_table_js(table: Table, column_name: &str) -> WasmResult<JsGeoArrowData> {
        let (schema, batches) = table.into_inner();
        let field = schema.field_with_name(column_name).map_err(|e| {
            JsError::new(&format!(
                "fromTable: column '{column_name}' not in schema: {e}"
            ))
        })?;
        if batches.is_empty() {
            return Err(JsError::new("fromTable: table has no record batches"));
        }
        let chunks = batches
            .iter()
            .map(|batch| {
                batch.column_by_name(column_name).cloned().ok_or_else(|| {
                    JsError::new(&format!(
                        "fromTable: column '{column_name}' missing from a batch"
                    ))
                })
            })
            .collect::<Result<Vec<ArrayRef>, _>>()?;
        let refs: Vec<&dyn Array> = chunks.iter().map(|a| a.as_ref()).collect();
        let combined = concat(&refs).map_err(|e| {
            JsError::new(&format!(
                "fromTable: cannot join the batches of '{column_name}': {e}"
            ))
        })?;
        Ok(Self(from_arrow_array(combined.as_ref(), field)?))
    }
}
