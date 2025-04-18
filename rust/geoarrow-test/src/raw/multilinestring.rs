use wkt::wkt;

pub mod xy {
    use super::*;

    pub fn geoms() -> Vec<Option<wkt::types::MultiLineString<f64>>> {
        vec![
            Some(wkt! { MULTILINESTRING ((30. 10., 10. 30., 40. 40.)) }),
            Some(
                wkt! { MULTILINESTRING ((10. 10., 20. 20., 10. 40.), (40. 40., 30. 30., 40. 20., 30. 10.)) },
            ),
            None,
            Some(wkt! { MULTILINESTRING EMPTY }),
        ]
    }
}

pub mod xyz {
    use super::*;

    pub fn geoms() -> Vec<Option<wkt::types::MultiLineString<f64>>> {
        vec![
            Some(wkt! { MULTILINESTRING Z ((30. 10. 40., 10. 30. 40., 40. 40. 80.)) }),
            Some(
                wkt! { MULTILINESTRING Z ((10. 10. 20., 20. 20. 40., 10. 40. 50.), (40. 40. 80., 30. 30. 60., 40. 20. 60., 30. 10. 40.)) },
            ),
            None,
            Some(wkt! { MULTILINESTRING Z EMPTY }),
        ]
    }
}

pub mod xym {
    use super::*;

    pub fn geoms() -> Vec<Option<wkt::types::MultiLineString<f64>>> {
        vec![
            Some(wkt! { MULTILINESTRING M ((30. 10. 300., 10. 30. 300., 40. 40. 1600.)) }),
            Some(
                wkt! { MULTILINESTRING M ((10. 10. 100., 20. 20. 400., 10. 40. 400.), (40. 40. 1600., 30. 30. 900., 40. 20. 800., 30. 10. 300.)) },
            ),
            None,
            Some(wkt! { MULTILINESTRING M EMPTY }),
        ]
    }
}

pub mod xyzm {
    use super::*;

    pub fn geoms() -> Vec<Option<wkt::types::MultiLineString<f64>>> {
        vec![
            Some(
                wkt! { MULTILINESTRING ZM ((30. 10. 40. 300., 10. 30. 40. 300., 40. 40. 80. 1600.)) },
            ),
            Some(
                wkt! { MULTILINESTRING ZM ((10. 10. 20. 100., 20. 20. 40. 400., 10. 40. 50. 400.), (40. 40. 80. 1600., 30. 30. 60. 900., 40. 20. 60. 800., 30. 10. 40. 300.)) },
            ),
            None,
            Some(wkt! { MULTILINESTRING ZM EMPTY }),
        ]
    }
}
