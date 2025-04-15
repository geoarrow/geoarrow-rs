use wkt::wkt;

pub mod xy {
    use super::*;

    pub fn geoms() -> Vec<Option<wkt::types::Polygon<i32>>> {
        vec![
            Some(wkt! { POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10)) }),
            Some(
                wkt! { POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10), (20 30, 35 35, 30 20, 20 30)) },
            ),
            None,
            Some(wkt! { POLYGON EMPTY }),
        ]
    }
}

pub mod xyz {
    use super::*;

    pub fn geoms() -> Vec<Option<wkt::types::Polygon<i32>>> {
        vec![
            Some(wkt! { POLYGON Z ((30 10 40, 40 40 80, 20 40 60, 10 20 30, 30 10 40)) }),
            Some(
                wkt! { POLYGON Z ((35 10 45, 45 45 90, 15 40 55, 10 20 30, 35 10 45), (20 30 50, 35 35 70, 30 20 50, 20 30 50)) },
            ),
            None,
            Some(wkt! { POLYGON Z EMPTY }),
        ]
    }
}

pub mod xym {
    use super::*;

    pub fn geoms() -> Vec<Option<wkt::types::Polygon<i32>>> {
        vec![
            Some(wkt! { POLYGON M ((30 10 300, 40 40 1600, 20 40 800, 10 20 200, 30 10 300)) }),
            Some(
                wkt! { POLYGON M ((35 10 350, 45 45 2025, 15 40 600, 10 20 200, 35 10 350), (20 30 600,
                35 35 1225, 30 20 600, 20 30 600)) },
            ),
            None,
            Some(wkt! { POLYGON M EMPTY }),
        ]
    }
}

pub mod xyzm {
    use super::*;

    pub fn geoms() -> Vec<Option<wkt::types::Polygon<i32>>> {
        vec![
            Some(
                wkt! { POLYGON ZM ((30 10 40 300, 40 40 80 1600, 20 40 60 800, 10 20 30 200, 30 10 40 300)) },
            ),
            Some(
                wkt! { POLYGON ZM ((35 10 45 350, 45 45 90 2025, 15 40 55 600, 10 20 30 200, 35 10 45 350), (20 30 50 600, 35 35 70 1225, 30 20 50 600, 20 30 50 600)) },
            ),
            None,
            Some(wkt! { POLYGON ZM EMPTY }),
        ]
    }
}
