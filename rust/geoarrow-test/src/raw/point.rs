use wkt::wkt;

pub mod xy {
    use super::*;

    pub fn geoms() -> Vec<Option<wkt::types::Point<i32>>> {
        vec![
            Some(wkt! { POINT (30 10) }),
            Some(wkt! { POINT (40 20) }),
            None,
            Some(wkt! { POINT EMPTY }),
        ]
    }
}

pub mod xyz {
    use super::*;

    pub fn geoms() -> Vec<Option<wkt::types::Point<i32>>> {
        vec![
            Some(wkt! { POINT Z (30 10 40) }),
            Some(wkt! { POINT Z (40 20 60) }),
            None,
            Some(wkt! { POINT Z EMPTY }),
        ]
    }
}

pub mod xym {
    use super::*;

    pub fn geoms() -> Vec<Option<wkt::types::Point<i32>>> {
        vec![
            Some(wkt! { POINT M (30 10 300) }),
            Some(wkt! { POINT M (40 20 800) }),
            None,
            Some(wkt! { POINT M EMPTY }),
        ]
    }
}

pub mod xyzm {
    use super::*;

    pub fn geoms() -> Vec<Option<wkt::types::Point<i32>>> {
        vec![
            Some(wkt! { POINT ZM (30 10 40 300) }),
            Some(wkt! { POINT ZM (40 20 60 800) }),
            None,
            Some(wkt! { POINT ZM EMPTY }),
        ]
    }
}
