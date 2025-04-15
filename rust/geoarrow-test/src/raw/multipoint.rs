use wkt::wkt;

pub mod xy {
    use super::*;

    pub fn geoms() -> Vec<Option<wkt::types::MultiPoint<i32>>> {
        vec![
            Some(wkt! { MULTIPOINT (30 10) }),
            Some(wkt! { MULTIPOINT (10 40, 40 30, 20 20, 30 10) }),
            None,
            Some(wkt! { MULTIPOINT EMPTY }),
        ]
    }
}

pub mod xyz {
    use super::*;

    pub fn geoms() -> Vec<Option<wkt::types::MultiPoint<i32>>> {
        vec![
            Some(wkt! { MULTIPOINT Z (30 10 40) }),
            Some(wkt! { MULTIPOINT Z (10 40 50, 40 30 70, 20 20 40, 30 10 40) }),
            None,
            Some(wkt! { MULTIPOINT Z EMPTY }),
        ]
    }
}

pub mod xym {
    use super::*;

    pub fn geoms() -> Vec<Option<wkt::types::MultiPoint<i32>>> {
        vec![
            Some(wkt! { MULTIPOINT M (30 10 300) }),
            Some(wkt! { MULTIPOINT M (10 40 400, 40 30 1200, 20 20 400, 30 10 300) }),
            None,
            Some(wkt! { MULTIPOINT M EMPTY }),
        ]
    }
}

pub mod xyzm {
    use super::*;

    pub fn geoms() -> Vec<Option<wkt::types::MultiPoint<i32>>> {
        vec![
            Some(wkt! { MULTIPOINT ZM (30 10 40 300) }),
            Some(wkt! { MULTIPOINT ZM (10 40 50 400, 40 30 70 1200, 20 20 40 400, 30 10 40 300) }),
            None,
            Some(wkt! { MULTIPOINT ZM EMPTY }),
        ]
    }
}
