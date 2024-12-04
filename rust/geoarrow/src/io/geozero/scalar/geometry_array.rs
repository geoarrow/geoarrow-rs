use crate::array::AsNativeArray;
use crate::datatypes::NativeType;
use crate::io::geozero::scalar::geometry_collection::process_geometry_collection;
use crate::io::geozero::scalar::linestring::process_line_string;
use crate::io::geozero::scalar::multilinestring::process_multi_line_string;
use crate::io::geozero::scalar::multipoint::process_multi_point;
use crate::io::geozero::scalar::multipolygon::process_multi_polygon;
use crate::io::geozero::scalar::point::process_point;
use crate::io::geozero::scalar::polygon::process_polygon;
use crate::io::geozero::scalar::process_geometry;
use crate::scalar::GeometryScalar;
use crate::trait_::ArrayAccessor;
use geozero::{GeomProcessor, GeozeroGeometry};

pub fn process_geometry_scalar_array<P: GeomProcessor>(
    geom: &GeometryScalar,
    geom_idx: usize,
    processor: &mut P,
) -> geozero::error::Result<()> {
    macro_rules! impl_process {
        ($process_func:ident, $cast_func:ident) => {
            $process_func(
                &geom.inner().as_ref().$cast_func().value(0),
                geom_idx,
                processor,
            )
        };
        ($process_func:ident, true, $cast_func:ident) => {
            $process_func(
                &geom.inner().as_ref().$cast_func().value(0),
                true,
                geom_idx,
                processor,
            )
        };
    }

    use NativeType::*;

    match geom.data_type() {
        Point(_, _) => impl_process!(process_point, as_point),
        LineString(_, _) => impl_process!(process_line_string, as_line_string),
        Polygon(_, _) => impl_process!(process_polygon, true, as_polygon),
        MultiPoint(_, _) => impl_process!(process_multi_point, as_multi_point),
        MultiLineString(_, _) => impl_process!(process_multi_line_string, as_multi_line_string),
        MultiPolygon(_, _) => impl_process!(process_multi_polygon, as_multi_polygon),
        Mixed(_, _) => impl_process!(process_geometry, as_mixed),
        GeometryCollection(_, _) => {
            impl_process!(process_geometry_collection, as_geometry_collection)
        }

        // WKB => {
        //     let arr = &geom.inner().as_ref();
        //     let wkb_arr = arr.as_wkb().value(0);
        //     let wkb_object = wkb_arr.to_wkb_object();
        //     process_geometry(&wkb_object, geom_idx, processor)
        // }
        // LargeWKB => {
        //     let arr = &geom.inner().as_ref();
        //     let wkb_arr = arr.as_large_wkb().value(0);
        //     let wkb_object = wkb_arr.to_wkb_object();
        //     process_geometry(&wkb_object, geom_idx, processor)
        // }
        Rect(_) => todo!(),
        Geometry(_) => impl_process!(process_geometry, as_geometry),
    }
}

impl GeozeroGeometry for GeometryScalar {
    fn process_geom<P: GeomProcessor>(&self, processor: &mut P) -> geozero::error::Result<()>
    where
        Self: Sized,
    {
        process_geometry_scalar_array(self, 0, processor)
    }
}
