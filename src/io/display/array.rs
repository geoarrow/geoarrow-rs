use std::fmt::{self, Write};

use arrow_array::OffsetSizeTrait;

use crate::array::*;
use crate::io::display::scalar::write_geometry;
use crate::trait_::{ArrayAccessor, NativeScalar};

pub(crate) fn indent(f: &mut fmt::Formatter<'_>, indented_spaces: usize) -> fmt::Result {
    (0..indented_spaces).try_for_each(|_| f.write_char(' '))
}

pub(crate) fn write_indented_geom(
    f: &mut fmt::Formatter<'_>,
    geom: Option<geo::Geometry>,
    indented_spaces: usize,
) -> fmt::Result {
    indent(f, indented_spaces)?;
    if let Some(geom) = geom {
        write_geometry(f, geom, 80 - indented_spaces - 1)?;
        writeln!(f, ",")?;
    } else {
        writeln!(f, "null,")?;
    }

    Ok(())
}

pub(crate) fn write_indented_ellipsis(
    f: &mut fmt::Formatter<'_>,
    indented_spaces: usize,
) -> fmt::Result {
    indent(f, indented_spaces)?;
    writeln!(f, "...,")
}

/// Extra trait so that the chunked array can reuse this with indented spaces
pub(crate) trait WriteArray {
    fn write(&self, f: &mut fmt::Formatter<'_>, indented_spaces: usize) -> fmt::Result;
}

macro_rules! impl_fmt_non_generic {
    ($struct_name:ty, $str_literal:tt) => {
        impl WriteArray for $struct_name {
            fn write(&self, f: &mut fmt::Formatter<'_>, indented_spaces: usize) -> fmt::Result {
                indent(f, indented_spaces)?;
                f.write_str($str_literal)?;
                writeln!(f, "([")?;

                if self.len() > 6 {
                    for maybe_geom in self.iter().take(3) {
                        write_indented_geom(
                            f,
                            maybe_geom.map(|g| g.to_geo_geometry()),
                            indented_spaces + 4,
                        )?;
                    }
                    write_indented_ellipsis(f, indented_spaces + 4)?;
                    for maybe_geom in self.slice(self.len() - 3, 3).iter() {
                        write_indented_geom(
                            f,
                            maybe_geom.map(|g| g.to_geo_geometry()),
                            indented_spaces + 4,
                        )?;
                    }
                } else {
                    for maybe_geom in self.iter() {
                        write_indented_geom(
                            f,
                            maybe_geom.map(|g| g.to_geo_geometry()),
                            indented_spaces + 4,
                        )?;
                    }
                }
                indent(f, indented_spaces)?;
                write!(f, "])")?;
                Ok(())
            }
        }
    };
}

impl_fmt_non_generic!(PointArray<2>, "PointArray");
impl_fmt_non_generic!(RectArray<2>, "RectArray");

macro_rules! impl_fmt_generic {
    ($struct_name:ty, $str_literal:tt) => {
        impl<O: OffsetSizeTrait> WriteArray for $struct_name {
            fn write(&self, f: &mut fmt::Formatter<'_>, indented_spaces: usize) -> fmt::Result {
                indent(f, indented_spaces)?;
                f.write_str(O::PREFIX)?;
                f.write_str($str_literal)?;
                writeln!(f, "([")?;

                if self.len() > 6 {
                    for maybe_geom in self.iter().take(3) {
                        write_indented_geom(
                            f,
                            maybe_geom.map(|g| g.to_geo_geometry()),
                            indented_spaces + 4,
                        )?;
                    }
                    write_indented_ellipsis(f, indented_spaces + 4)?;
                    for maybe_geom in self.slice(self.len() - 3, 3).iter() {
                        write_indented_geom(
                            f,
                            maybe_geom.map(|g| g.to_geo_geometry()),
                            indented_spaces + 4,
                        )?;
                    }
                } else {
                    for maybe_geom in self.iter() {
                        write_indented_geom(
                            f,
                            maybe_geom.map(|g| g.to_geo_geometry()),
                            indented_spaces + 4,
                        )?;
                    }
                }
                indent(f, indented_spaces)?;
                write!(f, "])")?;
                Ok(())
            }
        }
    };
}

impl_fmt_generic!(LineStringArray<O, 2>, "LineStringArray");
impl_fmt_generic!(PolygonArray<O, 2>, "PolygonArray");
impl_fmt_generic!(MultiPointArray<O, 2>, "MultiPointArray");
impl_fmt_generic!(MultiLineStringArray<O, 2>, "MultiLineStringArray");
impl_fmt_generic!(MultiPolygonArray<O, 2>, "MultiPolygonArray");
impl_fmt_generic!(MixedGeometryArray<O, 2>, "MixedGeometryArray");
impl_fmt_generic!(GeometryCollectionArray<O, 2>, "GeometryCollectionArray");
impl_fmt_generic!(WKBArray<O>, "WKBArray");

impl fmt::Display for PointArray<2> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.write(f, 0)
    }
}

impl fmt::Display for RectArray<2> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.write(f, 0)
    }
}

macro_rules! impl_fmt {
    ($struct_name:ty, $str_literal:tt) => {
        impl<O: OffsetSizeTrait> fmt::Display for $struct_name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.write(f, 0)
            }
        }
    };
}

impl_fmt!(LineStringArray<O, 2>, "LineStringArray");
impl_fmt!(PolygonArray<O, 2>, "PolygonArray");
impl_fmt!(MultiPointArray<O, 2>, "MultiPointArray");
impl_fmt!(MultiLineStringArray<O, 2>, "MultiLineStringArray");
impl_fmt!(MultiPolygonArray<O, 2>, "MultiPolygonArray");
impl_fmt!(MixedGeometryArray<O, 2>, "MixedGeometryArray");
impl_fmt!(GeometryCollectionArray<O, 2>, "GeometryCollectionArray");
impl_fmt!(WKBArray<O>, "WKBArray");

#[cfg(test)]
mod test {
    use crate::io::wkb::ToWKB;
    use crate::test::{linestring, point};
    use crate::NativeArray;

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
