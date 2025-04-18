use geo_traits::MultiLineStringTrait;

use crate::export::scalar::linestring::to_geos_line_string;

pub(crate) fn to_geos_multi_line_string(
    multi_line_string: &impl MultiLineStringTrait<T = f64>,
) -> std::result::Result<geos::Geometry, geos::Error> {
    geos::Geometry::create_multiline_string(
        multi_line_string
            .line_strings()
            .map(|line| to_geos_line_string(&line))
            .collect::<std::result::Result<Vec<_>, geos::Error>>()?,
    )
}
