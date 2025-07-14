use geozero::{GeomProcessor, GeozeroGeometry};

use crate::array::AsNativeArray;
use crate::array::dynamic::NativeArrayDyn;
use crate::datatypes::NativeType;

impl GeozeroGeometry for NativeArrayDyn {
    fn process_geom<P: GeomProcessor>(&self, processor: &mut P) -> geozero::error::Result<()>
    where
        Self: Sized,
    {
        macro_rules! impl_process {
            ($cast_func:ident) => {{
                let arr = self.inner().as_ref();
                arr.$cast_func().process_geom(processor)
            }};
        }

        use NativeType::*;

        match self.inner().data_type() {
            Point(_) => impl_process!(as_point),
            LineString(_) => impl_process!(as_line_string),
            Polygon(_) => impl_process!(as_polygon),
            MultiPoint(_) => impl_process!(as_multi_point),
            MultiLineString(_) => {
                impl_process!(as_multi_line_string)
            }
            MultiPolygon(_) => impl_process!(as_multi_polygon),
            GeometryCollection(_) => {
                impl_process!(as_geometry_collection)
            }
            _ => todo!(),
            // WKB => impl_process!(as_wkb),
            // Rect(_) => impl_process!(as_rect)
            // Rect(XYZ) => impl_process!(as_rect, 3)
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use geozero::ToGeo;

    use super::*;
    use crate::ArrayBase;
    use crate::array::PointArray;
    use crate::test::point;

    #[test]
    fn test() {
        let arr = point::point_array();
        let geom_arr = NativeArrayDyn::new(Arc::new(arr));
        let test = geom_arr.as_any().downcast_ref::<PointArray>().unwrap();
        dbg!(geom_arr.to_geo().unwrap());
        dbg!(test);
    }
}
