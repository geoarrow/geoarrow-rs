use arrow2::array::PrimitiveArray;
use arrow2::bitmap::Bitmap;
use arrow2::datatypes::DataType;

pub(crate) fn zeroes(len: usize, validity: Option<&Bitmap>) -> PrimitiveArray<f64> {
    let values = vec![0.0f64; len];
    PrimitiveArray::new(DataType::Float64, values.into(), validity.cloned())
}
