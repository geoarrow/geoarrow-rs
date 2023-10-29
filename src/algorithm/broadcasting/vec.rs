/// An enum over any arbitrary object, where a [`Vec`] is used to hold the Array variant.
///
/// [`IntoIterator`] is implemented for this, where it will iterate over the `Array` variant
/// normally but will iterate over the `Scalar` variant forever.
#[derive(Debug, Clone)]
pub enum BroadcastableVec<T>
where
    T: Clone,
{
    Scalar(T),
    Array(Vec<T>),
}

pub enum BroadcastIter<'a, T> {
    Scalar(T),
    // TODO: switch this to a ZipValidity that yields option values
    // Array(ZipValidity<&'a T, std::slice::Iter<'a, T>, BitIterator<'a>>),
    Array(std::slice::Iter<'a, T>),
}

impl<'a, T> IntoIterator for &'a BroadcastableVec<T>
where
    T: Clone,
{
    type Item = T;
    type IntoIter = BroadcastIter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            BroadcastableVec::Array(arr) => BroadcastIter::Array(arr.iter()),
            BroadcastableVec::Scalar(val) => BroadcastIter::Scalar(val.clone()),
        }
    }
}

impl<'a, T> Iterator for BroadcastIter<'a, T>
where
    T: Clone,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            BroadcastIter::Array(arr) => arr.next().cloned(),
            BroadcastIter::Scalar(val) => Some(val.to_owned()),
        }
    }
}
