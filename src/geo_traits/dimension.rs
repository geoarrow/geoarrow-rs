#![allow(clippy::upper_case_acronyms)]

use geo::CoordNum;

pub trait DimensionXY {
    type T: CoordNum;

    fn x(&self) -> Self::T;
    fn y(&self) -> Self::T;
}

pub trait DimensionXYZ: DimensionXY {
    fn z(&self) -> Self::T;
}

pub trait Test1<const N: usize> {}

pub const xy: usize = 1;

pub struct A {}
impl Test1<xy> for A {}

pub trait Dimension2 {
    const HAS_Z: bool;
    const HAS_M: bool;
}

pub struct XY {}
pub struct XYZ {}
pub struct XYM {}
pub struct XYZM {}

impl Dimension2 for XY {
    const HAS_Z: bool = false;
    const HAS_M: bool = false;
}
impl Dimension2 for XYZ {
    const HAS_Z: bool = true;
    const HAS_M: bool = false;
}
impl Dimension2 for XYM {
    const HAS_Z: bool = false;
    const HAS_M: bool = true;
}
impl Dimension2 for XYZM {
    const HAS_Z: bool = true;
    const HAS_M: bool = true;
}

// https://rust-lang.github.io/api-guidelines/future-proofing.html#sealed-traits-protect-against-downstream-implementations-c-sealed
mod private {
    use super::*;

    pub trait Sealed {}

    impl Sealed for XY {}
    impl Sealed for XYZ {}
    impl Sealed for XYM {}
    impl Sealed for XYZM {}
}
