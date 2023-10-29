use arrow_array::iterator::ArrayIter;
use arrow_array::types::ArrowPrimitiveType;
use arrow_array::PrimitiveArray;

/// An enum over primitive types defined by [`arrow2::types::NativeType`]. These include u8, i32,
/// f64, etc.
///
/// [`IntoIterator`] is implemented for this, where it will iterate over the `Array` variant
/// normally but will iterate over the `Scalar` variant forever.
#[derive(Debug, Clone)]
pub enum BroadcastablePrimitive<T>
where
    T: ArrowPrimitiveType,
{
    Scalar(T::Native),
    Array(PrimitiveArray<T>),
}

pub enum BroadcastIter<'a, T: ArrowPrimitiveType> {
    Scalar(T::Native),
    Array(ArrayIter<&'a PrimitiveArray<T>>),
}

impl<'a, T> IntoIterator for &'a BroadcastablePrimitive<T>
where
    T: ArrowPrimitiveType,
{
    type Item = Option<T::Native>;
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
    T: ArrowPrimitiveType,
{
    type Item = Option<T::Native>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            BroadcastIter::Array(arr) => arr.next(),
            BroadcastIter::Scalar(val) => Some(Some(val.to_owned())),
        }
    }
}
