use wkt::wkt;

pub mod xy {
    use super::*;

    pub fn geoms() -> Vec<Option<wkt::types::GeometryCollection<i32>>> {
        vec![
            Some(wkt! { GEOMETRYCOLLECTION (POINT (30 10)) }),
            Some(wkt! { GEOMETRYCOLLECTION (LINESTRING (30 10, 10 30, 40 40)) }),
            Some(wkt! { GEOMETRYCOLLECTION (POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))) }),
            Some(wkt! { GEOMETRYCOLLECTION (MULTIPOINT (30 10)) }),
            Some(wkt! { GEOMETRYCOLLECTION (MULTILINESTRING ((30 10, 10 30, 40 40))) }),
            Some(
                wkt! { GEOMETRYCOLLECTION (MULTIPOLYGON (((30 10, 40 40, 20 40, 10 20, 30 10)))) },
            ),
            Some(
                wkt! { GEOMETRYCOLLECTION (POINT (30 10), LINESTRING (30 10, 10 30, 40 40), POLYGON ((30
                10, 40 40, 20 40, 10 20, 30 10)), MULTIPOINT (30 10), MULTILINESTRING ((30 10,
                10 30, 40 40)), MULTIPOLYGON (((30 10, 40 40, 20 40, 10 20, 30 10)))) },
            ),
            None,
            Some(wkt! { GEOMETRYCOLLECTION EMPTY }),
        ]
    }
}

pub mod xyz {
    use super::*;

    pub fn geoms() -> Vec<Option<wkt::types::GeometryCollection<i32>>> {
        vec![
            Some(wkt! { GEOMETRYCOLLECTION Z (POINT Z (30 10 40)) }),
            Some(wkt! { GEOMETRYCOLLECTION Z (LINESTRING Z (30 10 40, 10 30 40, 40 40 80)) }),
            Some(
                wkt! { GEOMETRYCOLLECTION Z (POLYGON Z ((30 10 40, 40 40 80, 20 40 60, 10 20 30, 30 10 40))) },
            ),
            Some(wkt! { GEOMETRYCOLLECTION Z (MULTIPOINT Z (30 10 40)) }),
            Some(
                wkt! { GEOMETRYCOLLECTION Z (MULTILINESTRING Z ((30 10 40, 10 30 40, 40 40 80))) },
            ),
            Some(
                wkt! { GEOMETRYCOLLECTION Z (MULTIPOLYGON Z (((30 10 40, 40 40 80, 20 40 60, 10 20 30, 30 10 40)))) },
            ),
            Some(
                wkt! { GEOMETRYCOLLECTION Z (POINT Z (30 10 40), LINESTRING Z (30 10 40, 10 30 40, 40 40 80), POLYGON Z ((30 10 40, 40 40 80, 20 40 60, 10 20 30, 30 10 40)), MULTIPOINT Z (30 10 40), MULTILINESTRING Z ((30 10 40, 10 30 40, 40 40 80)), MULTIPOLYGON Z (((30 10 40, 40 40 80, 20 40 60, 10 20 30, 30 10 40)))) },
            ),
            None,
            Some(wkt! { GEOMETRYCOLLECTION Z EMPTY }),
        ]
    }
}

pub mod xym {
    use super::*;

    pub fn geoms() -> Vec<Option<wkt::types::GeometryCollection<i32>>> {
        vec![
            Some(wkt! { GEOMETRYCOLLECTION M (POINT M (30 10 300)) }),
            Some(wkt! { GEOMETRYCOLLECTION M (LINESTRING M (30 10 300, 10 30 300, 40 40 1600)) }),
            Some(
                wkt! { GEOMETRYCOLLECTION M (POLYGON M ((30 10 300, 40 40 1600, 20 40 800, 10 20 200, 30 10 300))) },
            ),
            Some(wkt! { GEOMETRYCOLLECTION M (MULTIPOINT M (30 10 300)) }),
            Some(
                wkt! { GEOMETRYCOLLECTION M (MULTILINESTRING M ((30 10 300, 10 30 300, 40 40 1600))) },
            ),
            Some(
                wkt! { GEOMETRYCOLLECTION M (MULTIPOLYGON M (((30 10 300, 40 40 1600, 20 40 800, 10 20 200, 30 10 300)))) },
            ),
            Some(
                wkt! { GEOMETRYCOLLECTION M (POINT M (30 10 300), LINESTRING M (30 10 300, 10 30 300, 40 40 1600), POLYGON M ((30 10 300, 40 40 1600, 20 40 800, 10 20 200, 30 10 300)), MULTIPOINT M (30 10 300), MULTILINESTRING M ((30 10 300, 10 30 300, 40 40 1600)), MULTIPOLYGON M (((30 10 300, 40 40 1600, 20 40 800, 10 20 200, 30 10 300)))) },
            ),
            None,
            Some(wkt! { GEOMETRYCOLLECTION M EMPTY }),
        ]
    }
}

pub mod xyzm {
    use super::*;

    pub fn geoms() -> Vec<Option<wkt::types::GeometryCollection<i32>>> {
        vec![
            Some(wkt! { GEOMETRYCOLLECTION ZM (POINT ZM (30 10 40 300)) }),
            Some(
                wkt! { GEOMETRYCOLLECTION ZM (LINESTRING ZM (30 10 40 300, 10 30 40 300, 40 40 80 1600)) },
            ),
            Some(
                wkt! { GEOMETRYCOLLECTION ZM (POLYGON ZM ((30 10 40 300, 40 40 80 1600, 20 40 60 800, 10 20 30 200, 30 10 40 300))) },
            ),
            Some(wkt! { GEOMETRYCOLLECTION ZM (MULTIPOINT ZM (30 10 40 300)) }),
            Some(
                wkt! { GEOMETRYCOLLECTION ZM (MULTILINESTRING ZM ((30 10 40 300, 10 30 40 300, 40 40 80 1600))) },
            ),
            Some(
                wkt! { GEOMETRYCOLLECTION ZM (MULTIPOLYGON ZM (((30 10 40 300, 40 40 80 1600, 20 40 60 800, 10 20 30 200, 30 10 40 300)))) },
            ),
            Some(
                wkt! { GEOMETRYCOLLECTION ZM (POINT ZM (30 10 40 300), LINESTRING ZM (30 10 40 300, 10
                30 40 300, 40 40 80 1600), POLYGON ZM ((30 10 40 300, 40 40 80 1600, 20 40 60 800,
                10 20 30 200, 30 10 40 300)), MULTIPOINT ZM (30 10 40 300), MULTILINESTRING ZM
                ((30 10 40 300, 10 30 40 300, 40 40 80 1600)), MULTIPOLYGON ZM (((30 10 40 300,
                40 40 80 1600, 20 40 60 800, 10 20 30 200, 30 10 40 300)))) },
            ),
            None,
            Some(wkt! { GEOMETRYCOLLECTION ZM EMPTY }),
        ]
    }
}
