use crate::array::AsGeometryArray;
use crate::datatypes::{Dimension, GeoDataType};
use crate::io::geozero::scalar::geometry_collection::process_geometry_collection;
use crate::io::geozero::scalar::linestring::process_line_string;
use crate::io::geozero::scalar::multilinestring::process_multi_line_string;
use crate::io::geozero::scalar::multipoint::process_multi_point;
use crate::io::geozero::scalar::multipolygon::process_multi_polygon;
use crate::io::geozero::scalar::point::process_point;
use crate::io::geozero::scalar::polygon::process_polygon;
use crate::io::geozero::scalar::process_geometry;
use crate::scalar::GeometryScalarArray;
use crate::trait_::GeometryArrayAccessor;
use geozero::{GeomProcessor, GeozeroGeometry};

pub fn process_geometry_scalar_array<P: GeomProcessor>(
    geom: &GeometryScalarArray,
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

    use Dimension::*;
    use GeoDataType::*;

    match geom.data_type() {
        Point(_, XY) => impl_process!(process_point, as_point_2d),
        LineString(_, XY) => impl_process!(process_line_string, as_line_string_2d),
        LargeLineString(_, XY) => impl_process!(process_line_string, as_large_line_string_2d),
        Polygon(_, XY) => impl_process!(process_polygon, true, as_polygon_2d),
        LargePolygon(_, XY) => impl_process!(process_polygon, true, as_large_polygon_2d),
        MultiPoint(_, XY) => impl_process!(process_multi_point, as_multi_point_2d),
        LargeMultiPoint(_, XY) => impl_process!(process_multi_point, as_large_multi_point_2d),
        MultiLineString(_, XY) => impl_process!(process_multi_line_string, as_multi_line_string_2d),
        LargeMultiLineString(_, XY) => {
            impl_process!(process_multi_line_string, as_large_multi_line_string_2d)
        }
        MultiPolygon(_, XY) => impl_process!(process_multi_polygon, as_multi_polygon_2d),
        LargeMultiPolygon(_, XY) => impl_process!(process_multi_polygon, as_large_multi_polygon_2d),
        Mixed(_, XY) => impl_process!(process_geometry, as_mixed_2d),
        LargeMixed(_, XY) => impl_process!(process_geometry, as_large_mixed_2d),
        GeometryCollection(_, XY) => {
            impl_process!(process_geometry_collection, as_geometry_collection_2d)
        }
        LargeGeometryCollection(_, XY) => {
            impl_process!(process_geometry_collection, as_large_geometry_collection_2d)
        }
        Point(_, XYZ) => impl_process!(process_point, as_point_3d),
        LineString(_, XYZ) => impl_process!(process_line_string, as_line_string_3d),
        LargeLineString(_, XYZ) => impl_process!(process_line_string, as_large_line_string_3d),
        Polygon(_, XYZ) => impl_process!(process_polygon, true, as_polygon_3d),
        LargePolygon(_, XYZ) => impl_process!(process_polygon, true, as_large_polygon_3d),
        MultiPoint(_, XYZ) => impl_process!(process_multi_point, as_multi_point_3d),
        LargeMultiPoint(_, XYZ) => impl_process!(process_multi_point, as_large_multi_point_3d),
        MultiLineString(_, XYZ) => {
            impl_process!(process_multi_line_string, as_multi_line_string_3d)
        }
        LargeMultiLineString(_, XYZ) => {
            impl_process!(process_multi_line_string, as_large_multi_line_string_3d)
        }
        MultiPolygon(_, XYZ) => impl_process!(process_multi_polygon, as_multi_polygon_3d),
        LargeMultiPolygon(_, XYZ) => {
            impl_process!(process_multi_polygon, as_large_multi_polygon_3d)
        }
        Mixed(_, XYZ) => impl_process!(process_geometry, as_mixed_3d),
        LargeMixed(_, XYZ) => impl_process!(process_geometry, as_large_mixed_3d),
        GeometryCollection(_, XYZ) => {
            impl_process!(process_geometry_collection, as_geometry_collection_3d)
        }
        LargeGeometryCollection(_, XYZ) => {
            impl_process!(process_geometry_collection, as_large_geometry_collection_3d)
        }
        WKB => {
            let arr = &geom.inner().as_ref();
            let wkb_arr = arr.as_wkb().value(0);
            let wkb_object = wkb_arr.to_wkb_object();
            process_geometry(&wkb_object, geom_idx, processor)
        }
        LargeWKB => {
            let arr = &geom.inner().as_ref();
            let wkb_arr = arr.as_large_wkb().value(0);
            let wkb_object = wkb_arr.to_wkb_object();
            process_geometry(&wkb_object, geom_idx, processor)
        }
        Rect(_) => todo!(),
    }
}

impl GeozeroGeometry for GeometryScalarArray {
    fn process_geom<P: GeomProcessor>(&self, processor: &mut P) -> geozero::error::Result<()>
    where
        Self: Sized,
    {
        process_geometry_scalar_array(self, 0, processor)
    }
}
