use geozero::{GeomProcessor, GeozeroGeometry};

use crate::array::dynamic::NativeArrayDyn;
use crate::array::AsNativeArray;
use crate::datatypes::{Dimension, NativeType};

impl GeozeroGeometry for NativeArrayDyn {
    fn process_geom<P: GeomProcessor>(&self, processor: &mut P) -> geozero::error::Result<()>
    where
        Self: Sized,
    {
        macro_rules! impl_process {
            ($cast_func:ident, $dim:expr) => {{
                let arr = self.inner().as_ref();
                arr.$cast_func::<$dim>().process_geom(processor)
            }};
            ($cast_func:ident) => {{
                let arr = self.inner().as_ref();
                arr.$cast_func().process_geom(processor)
            }};
        }

        use Dimension::*;
        use NativeType::*;

        match self.inner().data_type() {
            Point(_, XY) => impl_process!(as_point, 2),
            LineString(_, XY) => impl_process!(as_line_string, 2),
            Polygon(_, XY) => impl_process!(as_polygon, 2),
            MultiPoint(_, XY) => impl_process!(as_multi_point, 2),
            MultiLineString(_, XY) => {
                impl_process!(as_multi_line_string, 2)
            }
            MultiPolygon(_, XY) => impl_process!(as_multi_polygon, 2),
            Mixed(_, XY) => impl_process!(as_mixed, 2),
            GeometryCollection(_, XY) => {
                impl_process!(as_geometry_collection, 2)
            }
            Point(_, XYZ) => impl_process!(as_point, 3),
            LineString(_, XYZ) => impl_process!(as_line_string, 3),
            Polygon(_, XYZ) => impl_process!(as_polygon, 3),
            MultiPoint(_, XYZ) => impl_process!(as_multi_point, 3),
            MultiLineString(_, XYZ) => {
                impl_process!(as_multi_line_string, 3)
            }
            MultiPolygon(_, XYZ) => impl_process!(as_multi_polygon, 3),
            Mixed(_, XYZ) => impl_process!(as_mixed, 3),
            GeometryCollection(_, XYZ) => {
                impl_process!(as_geometry_collection, 3)
            }
            _ => todo!(),
            // WKB => impl_process!(as_wkb),
            // Rect(XY) => impl_process!(as_rect, 2)
            // Rect(XYZ) => impl_process!(as_rect, 3)
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use geozero::ToGeo;

    use super::*;
    use crate::array::PointArray;
    use crate::test::point;
    use crate::ArrayBase;

    #[test]
    fn test() {
        let arr = point::point_array();
        let geom_arr = NativeArrayDyn::new(Arc::new(arr));
        let test = geom_arr.as_any().downcast_ref::<PointArray<2>>().unwrap();
        dbg!(geom_arr.to_geo().unwrap());
        dbg!(test);
    }
}
