use arrow_array::Float64Array;
use arrow_buffer::NullBuffer;

pub(crate) fn zeroes(len: usize, nulls: Option<&NullBuffer>) -> Float64Array {
    let values = vec![0.0f64; len];
    Float64Array::new(values.into(), nulls.cloned())
}
