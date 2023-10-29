use arrow_array::{StringArray, UInt8Array};

pub(crate) fn u8_array() -> UInt8Array {
    UInt8Array::from(vec![1, 2, 3])
}

pub(crate) fn string_array() -> StringArray {
    vec!["foo", "bar", "baz"].into()
}
