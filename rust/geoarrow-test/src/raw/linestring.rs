use wkt::wkt;

pub mod xy {
    use super::*;

    pub fn geoms() -> Vec<Option<wkt::types::LineString<i32>>> {
        vec![
            Some(wkt! { LINESTRING (30 10, 10 30, 40 40) }),
            Some(wkt! { LINESTRING (40 20, 20 40, 50 50) }),
            None,
            Some(wkt! { LINESTRING EMPTY }),
        ]
    }
}

pub mod xyz {
    use super::*;

    pub fn geoms() -> Vec<Option<wkt::types::LineString<i32>>> {
        vec![
            Some(wkt! { LINESTRING Z (30 10 40, 10 30 40, 40 40 80) }),
            Some(wkt! { LINESTRING Z (40 20 60, 20 40 60, 50 50 100) }),
            None,
            Some(wkt! { LINESTRING Z EMPTY }),
        ]
    }
}

pub mod xym {
    use super::*;

    pub fn geoms() -> Vec<Option<wkt::types::LineString<i32>>> {
        vec![
            Some(wkt! { LINESTRING M (30 10 300, 10 30 300, 40 40 1600) }),
            Some(wkt! { LINESTRING M (40 20 800, 20 40 800, 50 50 2500) }),
            None,
            Some(wkt! { LINESTRING M EMPTY }),
        ]
    }
}

pub mod xyzm {
    use super::*;

    pub fn geoms() -> Vec<Option<wkt::types::LineString<i32>>> {
        vec![
            Some(wkt! { LINESTRING ZM (30 10 40 300, 10 30 40 300, 40 40 80 1600) }),
            Some(wkt! { LINESTRING ZM (40 20 60 800, 20 40 60 800, 50 50 100 2500) }),
            None,
            Some(wkt! { LINESTRING ZM EMPTY }),
        ]
    }
}
