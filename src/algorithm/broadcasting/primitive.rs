use arrow_array::iterator::ArrayIter;
use arrow_array::types::ArrowPrimitiveType;
use arrow_array::{ArrayAccessor, PrimitiveArray};

/// An enum over primitive types defined by [`arrow2::types::NativeType`]. These include u8, i32,
/// f64, etc.
///
/// [`IntoIterator`] is implemented for this, where it will iterate over the `Array` variant
/// normally but will iterate over the `Scalar` variant forever.
#[derive(Debug, Clone)]
pub enum BroadcastablePrimitive<T>
where
    T: ArrowPrimitiveType + ArrayAccessor + Clone,
{
    Scalar(T),
    Array(PrimitiveArray<T>),
}

pub enum BroadcastIter<'a, T: ArrowPrimitiveType + ArrayAccessor + Clone> {
    Scalar(T),
    Array(ArrayIter<&'a PrimitiveArray<T>>),
}

impl<'a, T> IntoIterator for &'a BroadcastablePrimitive<T>
where
    T: ArrowPrimitiveType + ArrayAccessor + Clone,
{
    type Item = Option<T>;
    type IntoIter = BroadcastIter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            BroadcastablePrimitive::Array(arr) => BroadcastIter::Array(arr.iter()),
            BroadcastablePrimitive::Scalar(val) => BroadcastIter::Scalar(*val),
        }
    }
}

impl<'a, T> Iterator for BroadcastIter<'a, T>
where
    T: ArrowPrimitiveType + ArrayAccessor + Clone,
{
    type Item = Option<T>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            BroadcastIter::Array(arr) => todo!(), // arr.next() .copied(),
            BroadcastIter::Scalar(val) => Some(Some(val.to_owned())),
        }
    }
}
