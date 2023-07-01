use arrow_buffer::NullBuffer;

pub trait GeometryArrayTrait<'a> {
    type Scalar;
    // type ScalarGeo: From<Self::Scalar>;

    /// Access the value at slot `i` as an Arrow scalar, not considering validity.
    fn value(&'a self, i: usize) -> Self::Scalar;

    /// Access the value at slot `i` as an Arrow scalar, considering validity.
    fn get(&'a self, i: usize) -> Option<Self::Scalar> {
        if self.is_null(i) {
            return None;
        }

        Some(self.value(i))
    }

    // /// Access the value at slot `i` as a [`geo`] scalar, not considering validity.
    // fn value_as_geo(&'a self, i: usize) -> Self::ScalarGeo {
    //     self.value(i).into()
    // }

    // /// Access the value at slot `i` as a [`geo`] scalar, considering validity.
    // fn get_as_geo(&'a self, i: usize) -> Option<Self::ScalarGeo> {
    //     if self.is_null(i) {
    //         return None;
    //     }

    //     Some(self.value_as_geo(i))
    // }

    /// Access the array's null buffer.
    fn nulls(&self) -> Option<&NullBuffer>;

    /// Returns whether slot `i` is null.
    /// # Panic
    /// Panics iff `i >= self.len()`.
    #[inline]
    fn is_null(&self, i: usize) -> bool {
        self.nulls()
            .map(|x| x.is_null(i))
            .unwrap_or(false)
    }
}


pub trait GeometryScalarTrait<'a> {

}
