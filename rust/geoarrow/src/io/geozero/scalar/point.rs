use crate::scalar::Point;
use geo_traits::{CoordTrait, PointTrait};
use geozero::{GeomProcessor, GeozeroGeometry};

pub(crate) fn process_point<P: GeomProcessor>(
    geom: &impl PointTrait<T = f64>,
    geom_idx: usize,
    processor: &mut P,
) -> geozero::error::Result<()> {
    processor.point_begin(geom_idx)?;
    process_point_as_coord(geom, 0, processor)?;
    processor.point_end(geom_idx)?;
    Ok(())
}

/// Note that this does _not_ call `processor.point_begin` and `processor.point_end` because as of
/// geozero v0.12, `point_begin` and `point_end` are **not** called for each point in a
/// MultiPoint
/// https://github.com/georust/geozero/pull/183/files#diff-a583e23825ff28368eabfdbfdc362c6512e42097024d548fb18d88409feba76aR142-R143
pub(crate) fn process_point_as_coord<P: GeomProcessor>(
    geom: &impl PointTrait<T = f64>,
    coord_idx: usize,
    processor: &mut P,
) -> geozero::error::Result<()> {
    use geo_traits::Dimensions;

    // TODO: should revisit this now that we have a better separation between coord and point.
    // (ever since we split the point trait and coord trait again.)
    let coord = geom.coord().unwrap();

    match coord.dim() {
        Dimensions::Xy | Dimensions::Unknown(2) => processor.xy(coord.x(), coord.y(), coord_idx)?,
        Dimensions::Xyz | Dimensions::Unknown(3) => processor.coordinate(
            coord.x(),
            coord.y(),
            Some(coord.nth_unchecked(2)),
            None,
            None,
            None,
            coord_idx,
        )?,
        Dimensions::Xym => processor.coordinate(
            coord.x(),
            coord.y(),
            None,
            Some(coord.nth_unchecked(2)),
            None,
            None,
            coord_idx,
        )?,
        Dimensions::Xyzm | Dimensions::Unknown(4) => processor.coordinate(
            coord.x(),
            coord.y(),
            Some(coord.nth_unchecked(2)),
            Some(coord.nth_unchecked(3)),
            None,
            None,
            coord_idx,
        )?,
        d => panic!("Unexpected dimension {:?}", d),
    };
    Ok(())
}

impl GeozeroGeometry for Point<'_> {
    fn process_geom<P: GeomProcessor>(&self, processor: &mut P) -> geozero::error::Result<()>
    where
        Self: Sized,
    {
        process_point(self, 0, processor)
    }
}
