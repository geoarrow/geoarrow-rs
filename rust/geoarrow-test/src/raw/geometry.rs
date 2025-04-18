use wkt::wkt;

pub fn geoms() -> Vec<Option<wkt::Wkt<f64>>> {
    vec![
            Some(wkt! { POINT (30. 10.) }.into()),
            Some(wkt! { LINESTRING (30. 10., 10. 30., 40. 40.) }.into()),
            Some(wkt! { POLYGON ((30. 10., 40. 40., 20. 40., 10. 20., 30. 10.)) }.into()),
            Some(wkt! { MULTIPOINT (30. 10.) }.into()),
            Some(wkt! { MULTILINESTRING ((30. 10., 10. 30., 40. 40.)) }.into()),
            Some(wkt! { MULTIPOLYGON (((30. 10., 40. 40., 20. 40., 10. 20., 30. 10.))) }.into()),
            Some(wkt! { GEOMETRYCOLLECTION (POINT (30. 10.), LINESTRING (30. 10., 10. 30., 40. 40.), POLYGON ((30. 10., 40. 40., 20. 40., 10. 20., 30. 10.)), MULTIPOINT (30. 10.), MULTILINESTRING ((30. 10., 10. 30., 40. 40.)), MULTIPOLYGON (((30. 10., 40. 40., 20. 40., 10. 20., 30. 10.)))) }.into()),
            None,
            Some(wkt! { GEOMETRYCOLLECTION EMPTY }.into()),

            // Z
            Some(wkt! { POINT Z (30. 10. 40.) }.into()),
            Some(wkt! { LINESTRING Z (30. 10. 40., 10. 30. 40., 40. 40. 80.) }.into()),
            Some(wkt! { POLYGON Z ((30. 10. 40., 40. 40. 80., 20. 40. 60., 10. 20. 30., 30. 10. 40.)) }.into()),
            Some(wkt! { MULTIPOINT Z (30. 10. 40.) }.into()),
            Some(wkt! { MULTILINESTRING Z ((30. 10. 40., 10. 30. 40., 40. 40. 80.)) }.into()),
            Some(wkt! { MULTIPOLYGON Z (((30. 10. 40., 40. 40. 80., 20. 40. 60., 10. 20. 30., 30. 10. 40.))) }.into()),
            Some(wkt! { GEOMETRYCOLLECTION Z (POINT Z (30. 10. 40.), LINESTRING Z (30. 10. 40., 10. 30. 40., 40. 40. 80.), POLYGON Z ((30. 10. 40., 40. 40. 80., 20. 40. 60., 10. 20. 30., 30. 10. 40.)), MULTIPOINT Z (30. 10. 40.), MULTILINESTRING Z ((30. 10. 40., 10. 30. 40., 40. 40. 80.)), MULTIPOLYGON Z (((30. 10. 40., 40. 40. 80., 20. 40. 60., 10. 20. 30., 30. 10. 40.)))) }.into()),
            None,
            Some(wkt! { GEOMETRYCOLLECTION Z EMPTY }.into()),

            // M
            Some(wkt! { POINT M (30. 10. 300.) }.into()),
            Some(wkt! { LINESTRING M (30. 10. 300., 10. 30. 300., 40. 40. 1600.) }.into()),
            Some(wkt! { POLYGON M ((30. 10. 300., 40. 40. 1600., 20. 40. 800., 10. 20. 200., 30. 10. 300.)) }.into()),
            Some(wkt! { MULTIPOINT M (30. 10. 300.) }.into()),
            Some(wkt! { MULTILINESTRING M ((30. 10. 300., 10. 30. 300., 40. 40. 1600.)) }.into()),
            Some(wkt! { MULTIPOLYGON M (((30. 10. 300., 40. 40. 1600., 20. 40. 800., 10. 20. 200., 30. 10. 300.))) }.into()),
            Some(wkt! { GEOMETRYCOLLECTION M (POINT M (30. 10. 300.), LINESTRING M (30. 10. 300., 10. 30. 300., 40. 40. 1600.), POLYGON M ((30. 10. 300., 40. 40. 1600., 20. 40. 800., 10. 20. 200., 30. 10. 300.)), MULTIPOINT M (30. 10. 300.), MULTILINESTRING M ((30. 10. 300., 10. 30. 300., 40. 40. 1600.)), MULTIPOLYGON M (((30. 10. 300., 40. 40. 1600., 20. 40. 800., 10. 20. 200., 30. 10. 300.)))) }.into()),
            None,
            Some(wkt! { GEOMETRYCOLLECTION M EMPTY }.into()),

            // ZM
            Some(wkt! { POINT ZM (30. 10. 40. 300.) }.into()),
            Some(wkt! { LINESTRING ZM (30. 10. 40. 300., 10. 30. 40. 300., 40. 40. 80. 1600.) }.into()),
            Some(wkt! { POLYGON ZM ((30. 10. 40. 300., 40. 40. 80. 1600., 20. 40. 60. 800., 10. 20. 30. 200., 30. 10. 40. 300.)) }.into()),
            Some(wkt! { MULTIPOINT ZM (30. 10. 40. 300.) }.into()),
            Some(wkt! { MULTILINESTRING ZM ((30. 10. 40. 300., 10. 30. 40. 300., 40. 40. 80. 1600.)) }.into()),
            Some(wkt! { MULTIPOLYGON ZM (((30. 10. 40. 300., 40. 40. 80. 1600., 20. 40. 60. 800., 10. 20. 30. 200., 30. 10. 40. 300.))) }.into()),
            Some(wkt! { GEOMETRYCOLLECTION ZM (POINT ZM (30. 10. 40. 300.), LINESTRING ZM (30. 10. 40. 300., 10. 30. 40. 300., 40. 40. 80. 1600.), POLYGON ZM ((30. 10. 40. 300., 40. 40. 80. 1600., 20. 40. 60. 800., 10. 20. 30. 200., 30. 10. 40. 300.)), MULTIPOINT ZM (30. 10. 40. 300.), MULTILINESTRING ZM ((30. 10. 40. 300., 10. 30. 40. 300., 40. 40. 80. 1600.)), MULTIPOLYGON ZM (((30. 10. 40. 300., 40. 40. 80. 1600., 20. 40. 60. 800., 10. 20. 30. 200., 30. 10. 40. 300.)))) }.into() ),
            None,
            Some(wkt! { GEOMETRYCOLLECTION ZM EMPTY }.into()),
        ]
}
