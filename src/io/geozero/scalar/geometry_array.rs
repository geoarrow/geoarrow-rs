use crate::array::AsNativeArray;
use crate::datatypes::{Dimension, NativeType};
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
        ($process_func:ident, $cast_func:ident, $dim:expr) => {
            $process_func(
                &geom.inner().as_ref().$cast_func::<$dim>().value(0),
                geom_idx,
                processor,
            )
        };
        ($process_func:ident, true, $cast_func:ident, $dim:expr) => {
            $process_func(
                &geom.inner().as_ref().$cast_func::<$dim>().value(0),
                true,
                geom_idx,
                processor,
            )
        };
    }

    use Dimension::*;
    use NativeType::*;

    match geom.data_type() {
        Point(_, XY) => impl_process!(process_point, as_point, 2),
        LineString(_, XY) => impl_process!(process_line_string, as_line_string, 2),
        LargeLineString(_, XY) => impl_process!(process_line_string, as_large_line_string, 2),
        Polygon(_, XY) => impl_process!(process_polygon, true, as_polygon, 2),
        LargePolygon(_, XY) => impl_process!(process_polygon, true, as_large_polygon, 2),
        MultiPoint(_, XY) => impl_process!(process_multi_point, as_multi_point, 2),
        LargeMultiPoint(_, XY) => impl_process!(process_multi_point, as_large_multi_point, 2),
        MultiLineString(_, XY) => impl_process!(process_multi_line_string, as_multi_line_string, 2),
        LargeMultiLineString(_, XY) => {
            impl_process!(process_multi_line_string, as_large_multi_line_string, 2)
        }
        MultiPolygon(_, XY) => impl_process!(process_multi_polygon, as_multi_polygon, 2),
        LargeMultiPolygon(_, XY) => impl_process!(process_multi_polygon, as_large_multi_polygon, 2),
        Mixed(_, XY) => impl_process!(process_geometry, as_mixed, 2),
        LargeMixed(_, XY) => impl_process!(process_geometry, as_large_mixed, 2),
        GeometryCollection(_, XY) => {
            impl_process!(process_geometry_collection, as_geometry_collection, 2)
        }
        LargeGeometryCollection(_, XY) => {
            impl_process!(process_geometry_collection, as_large_geometry_collection, 2)
        }
        Point(_, XYZ) => impl_process!(process_point, as_point, 3),
        LineString(_, XYZ) => impl_process!(process_line_string, as_line_string, 3),
        LargeLineString(_, XYZ) => impl_process!(process_line_string, as_large_line_string, 3),
        Polygon(_, XYZ) => impl_process!(process_polygon, true, as_polygon, 3),
        LargePolygon(_, XYZ) => impl_process!(process_polygon, true, as_large_polygon, 3),
        MultiPoint(_, XYZ) => impl_process!(process_multi_point, as_multi_point, 3),
        LargeMultiPoint(_, XYZ) => impl_process!(process_multi_point, as_large_multi_point, 3),
        MultiLineString(_, XYZ) => {
            impl_process!(process_multi_line_string, as_multi_line_string, 3)
        }
        LargeMultiLineString(_, XYZ) => {
            impl_process!(process_multi_line_string, as_large_multi_line_string, 3)
        }
        MultiPolygon(_, XYZ) => impl_process!(process_multi_polygon, as_multi_polygon, 3),
        LargeMultiPolygon(_, XYZ) => {
            impl_process!(process_multi_polygon, as_large_multi_polygon, 3)
        }
        Mixed(_, XYZ) => impl_process!(process_geometry, as_mixed, 3),
        LargeMixed(_, XYZ) => impl_process!(process_geometry, as_large_mixed, 3),
        GeometryCollection(_, XYZ) => {
            impl_process!(process_geometry_collection, as_geometry_collection, 3)
        }
        LargeGeometryCollection(_, XYZ) => {
            impl_process!(process_geometry_collection, as_large_geometry_collection, 3)
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
