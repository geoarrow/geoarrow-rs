use geo::{point, MultiPoint};

pub(crate) fn mp0() -> MultiPoint {
    MultiPoint::new(vec![
        point!(
            x: 0., y: 1.
        ),
        point!(
            x: 1., y: 2.
        ),
    ])
}

pub(crate) fn mp1() -> MultiPoint {
    MultiPoint::new(vec![
        point!(
            x: 3., y: 4.
        ),
        point!(
            x: 5., y: 6.
        ),
    ])
}
