//! Contains the [`MultiLineStringArray`] and [`MutableMultiLineStringArray`] for arrays of
//! MultiLineString geometries.

#[cfg(feature = "geozero")]
pub use self::geozero::ToGeoArrowMultiLineStringArray;
pub use array::MultiLineStringArray;
pub use iterator::{MultiLineStringArrayValuesIter, MultiLineStringIterator};
pub use mutable::MutableMultiLineStringArray;

mod array;
#[cfg(feature = "geozero")]
mod geozero;
pub mod iterator;
mod mutable;
