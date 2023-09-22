use arrow2::array::{PrimitiveArray, Utf8Array};

pub(crate) fn u8_array() -> PrimitiveArray<u8> {
    PrimitiveArray::from_vec(vec![1, 2, 3])
}

pub(crate) fn string_array() -> Utf8Array<i32> {
    Utf8Array::from_slice(["foo", "bar", "baz"])
}
