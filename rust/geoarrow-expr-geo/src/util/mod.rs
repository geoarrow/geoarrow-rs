use std::sync::Arc;

use arrow_array::Float64Array;
use arrow_array::builder::Float64Builder;
use arrow_buffer::NullBuffer;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};
use geoarrow_schema::error::GeoArrowResult;

use crate::util::to_geo::geometry_to_geo;

pub(crate) mod downcast;
pub mod to_geo;

pub(crate) fn copy_geoarrow_array_ref(array: &dyn GeoArrowArray) -> Arc<dyn GeoArrowArray> {
    array.slice(0, array.len())
}

/// A zero for each row, keeping the null positions of the input.
pub(crate) fn zeros(len: usize, nulls: Option<NullBuffer>) -> Float64Array {
    let values = vec![0.0f64; len];
    Float64Array::new(values.into(), nulls)
}

/// Apply `f` to each geometry. A null row stays null.
pub(crate) fn map_to_f64<'a, F: Fn(&geo::Geometry) -> f64>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    f: F,
) -> GeoArrowResult<Float64Array> {
    let mut builder = Float64Builder::with_capacity(array.len());

    for item in array.iter() {
        if let Some(geom) = item {
            let geo_geom = geometry_to_geo(&geom?)?;
            builder.append_value(f(&geo_geom));
        } else {
            builder.append_null();
        }
    }

    Ok(builder.finish())
}
