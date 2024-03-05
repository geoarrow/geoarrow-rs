use std::fmt;

use arrow_array::OffsetSizeTrait;

use crate::array::*;
use crate::io::display::scalar::write_geometry;
use crate::trait_::{GeometryArrayAccessor, GeometryArraySelfMethods, GeometryScalarTrait};
use crate::GeometryArrayTrait;

fn write_indented_geom(f: &mut fmt::Formatter<'_>, geom: Option<geo::Geometry>) -> fmt::Result {
    write!(f, "    ")?;
    if let Some(geom) = geom {
        write_geometry(f, geom, 75)?;
        writeln!(f, ",")?;
    } else {
        writeln!(f, "null,")?;
    }

    Ok(())
}

fn write_indented_ellipsis(f: &mut fmt::Formatter<'_>) -> fmt::Result {
    writeln!(f, "    ...,")
}

impl fmt::Display for PointArray {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "PointArray([")?;

        if self.len() > 6 {
            for maybe_geom in self.iter().take(3) {
                write_indented_geom(f, maybe_geom.map(|g| g.to_geo_geometry()))?;
            }
            write_indented_ellipsis(f)?;
            for maybe_geom in self.slice(self.len() - 3, 3).iter() {
                write_indented_geom(f, maybe_geom.map(|g| g.to_geo_geometry()))?;
            }
        } else {
            for maybe_geom in self.iter() {
                write_indented_geom(f, maybe_geom.map(|g| g.to_geo_geometry()))?;
            }
        }
        write!(f, "])")?;
        Ok(())
    }
}

impl fmt::Display for RectArray {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "RectArray([")?;

        if self.len() > 6 {
            for maybe_geom in self.iter().take(3) {
                write_indented_geom(f, maybe_geom.map(|g| g.to_geo_geometry()))?;
            }
            write_indented_ellipsis(f)?;
            for maybe_geom in self.slice(self.len() - 3, 3).iter() {
                write_indented_geom(f, maybe_geom.map(|g| g.to_geo_geometry()))?;
            }
        } else {
            for maybe_geom in self.iter() {
                write_indented_geom(f, maybe_geom.map(|g| g.to_geo_geometry()))?;
            }
        }
        write!(f, "])")?;
        Ok(())
    }
}

macro_rules! impl_fmt {
    ($struct_name:ty, $str_literal:tt) => {
        impl<O: OffsetSizeTrait> fmt::Display for $struct_name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(O::PREFIX)?;
                f.write_str($str_literal)?;
                writeln!(f, "([")?;

                if self.len() > 6 {
                    for maybe_geom in self.iter().take(3) {
                        write_indented_geom(f, maybe_geom.map(|g| g.to_geo_geometry()))?;
                    }
                    write_indented_ellipsis(f)?;
                    for maybe_geom in self.slice(self.len() - 3, 3).iter() {
                        write_indented_geom(f, maybe_geom.map(|g| g.to_geo_geometry()))?;
                    }
                } else {
                    for maybe_geom in self.iter() {
                        write_indented_geom(f, maybe_geom.map(|g| g.to_geo_geometry()))?;
                    }
                }
                write!(f, "])")?;
                Ok(())
            }
        }
    };
}

impl_fmt!(LineStringArray<O>, "LineStringArray");
impl_fmt!(PolygonArray<O>, "PolygonArray");
impl_fmt!(MultiPointArray<O>, "MultiPointArray");
impl_fmt!(MultiLineStringArray<O>, "MultiLineStringArray");
impl_fmt!(MultiPolygonArray<O>, "MultiPolygonArray");
impl_fmt!(MixedGeometryArray<O>, "MixedGeometryArray");
impl_fmt!(GeometryCollectionArray<O>, "GeometryCollectionArray");
impl_fmt!(WKBArray<O>, "WKBArray");

#[cfg(test)]
mod test {
    use crate::io::wkb::ToWKB;
    use crate::test::{linestring, point};
    use crate::GeometryArrayTrait;

    #[test]
    fn test_display_point_array() {
        let point_array = point::point_array();
        let result = point_array.to_string();
        let expected = "PointArray([
    <POINT(0 1)>,
    <POINT(1 2)>,
    <POINT(2 3)>,
])";
        assert_eq!(result, expected);
    }

    #[test]
    fn test_display_ls_array() {
        let array = linestring::large_ls_array();
        let result = array.to_string();
        let expected = "LargeLineStringArray([
    <LINESTRING(0 1,1 2)>,
    <LINESTRING(3 4,5 6)>,
])";
        assert_eq!(result, expected);
    }

    #[test]
    fn test_display_wkb_array() {
        let array = point::point_array();
        let wkb_array = array.as_ref().to_wkb::<i32>();
        let result = wkb_array.to_string();
        let expected = "WKBArray([
    <POINT(0 1)>,
    <POINT(1 2)>,
    <POINT(2 3)>,
])";
        assert_eq!(result, expected);
    }
}
