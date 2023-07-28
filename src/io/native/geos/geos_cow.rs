use std::borrow::Borrow;
use std::ops::Deref;

pub enum GEOSCowGeometry<'a, B: ?Sized + 'a>
where
    B: ToOwned,
{
    /// Borrowed data.
    #[allow(dead_code)]
    Borrowed(&'a B),

    /// Owned data.
    #[allow(dead_code)]
    Owned(<B as ToOwned>::Owned),
}

impl<B: ?Sized + ToOwned> Clone for GEOSCowGeometry<'_, B> {
    fn clone(&self) -> Self {
        match *self {
            GEOSCowGeometry::Borrowed(b) => GEOSCowGeometry::Borrowed(b),
            GEOSCowGeometry::Owned(ref o) => {
                let b: &B = o.borrow();
                GEOSCowGeometry::Owned(b.to_owned())
            }
        }
    }

    fn clone_from(&mut self, source: &Self) {
        match (self, source) {
            (&mut GEOSCowGeometry::Owned(ref mut dest), GEOSCowGeometry::Owned(o)) => {
                o.borrow().clone_into(dest)
            }
            (t, s) => *t = s.clone(),
        }
    }
}

impl<B: ?Sized + ToOwned> GEOSCowGeometry<'_, B> {
    /// Extracts the owned data.
    ///
    /// Clones the data if it is not already owned.
    ///
    /// # Examples
    ///
    /// Calling `into_owned` on a `Cow::Borrowed` returns a clone of the borrowed data:
    ///
    /// ```
    /// use std::borrow::Cow;
    ///
    /// let s = "Hello world!";
    /// let cow = Cow::Borrowed(s);
    ///
    /// assert_eq!(
    ///   cow.into_owned(),
    ///   String::from(s)
    /// );
    /// ```
    ///
    /// Calling `into_owned` on a `Cow::Owned` returns the owned data. The data is moved out of the
    /// `Cow` without being cloned.
    ///
    /// ```
    /// use std::borrow::Cow;
    ///
    /// let s = "Hello world!";
    /// let cow: Cow<'_, str> = Cow::Owned(String::from(s));
    ///
    /// assert_eq!(
    ///   cow.into_owned(),
    ///   String::from(s)
    /// );
    /// ```
    #[allow(dead_code)]
    pub fn into_owned(self) -> <B as ToOwned>::Owned {
        match self {
            GEOSCowGeometry::Borrowed(borrowed) => borrowed.to_owned(),
            GEOSCowGeometry::Owned(owned) => owned,
        }
    }
}

impl<B: ?Sized + ToOwned> Deref for GEOSCowGeometry<'_, B>
where
    B::Owned: Borrow<B>,
{
    type Target = B;

    fn deref(&self) -> &B {
        match *self {
            GEOSCowGeometry::Borrowed(borrowed) => borrowed,
            GEOSCowGeometry::Owned(ref owned) => owned.borrow(),
        }
    }
}
