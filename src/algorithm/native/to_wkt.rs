use std::fmt::{Error, Write};

use arrow_array::{builder::GenericStringBuilder, GenericStringArray};

use crate::{
    array::AsNativeArray, datatypes::GeoDataType, geo_traits::*, trait_::NativeArrayAccessor,
    NativeArray,
};

/// Converts the input to WKT representation.
pub trait ToWKT {
    type Output;

    fn to_wkt(&self) -> Self::Output;
}

impl ToWKT for &dyn NativeArray {
    type Output = Result<GenericStringArray<i32>, Error>;

    fn to_wkt(&self) -> Self::Output {
        let mut wkt_builder: GenericStringBuilder<i32> = GenericStringBuilder::new();

        match self.data_type() {
            GeoDataType::Point(_coord_type, _dimension) => todo!(),
            GeoDataType::LineString(_coord_type, _dimension) => todo!(),
            GeoDataType::LargeLineString(_coord_type, _dimension) => todo!(),
            GeoDataType::Polygon(_coord_type, _dimension) => todo!(),
            GeoDataType::LargePolygon(_coord_type, _dimension) => todo!(),
            GeoDataType::MultiPoint(_coord_type, _dimension) => todo!(),
            GeoDataType::LargeMultiPoint(_coord_type, _dimension) => todo!(),
            GeoDataType::MultiLineString(_coord_type, _dimension) => todo!(),
            GeoDataType::LargeMultiLineString(_coord_type, _dimension) => todo!(),
            GeoDataType::MultiPolygon(_coord_type, _dimension) => todo!(),
            GeoDataType::LargeMultiPolygon(_coord_type, _dimension) => todo!(),
            GeoDataType::Mixed(_coord_type, _dimension) => todo!(),
            GeoDataType::LargeMixed(_coord_type, _dimension) => todo!(),
            GeoDataType::GeometryCollection(_coord_type, _dimension) => todo!(),
            GeoDataType::LargeGeometryCollection(_coord_type, _dimension) => todo!(),
            GeoDataType::WKB => {
                for item in self.as_wkb().iter() {
                    match item {
                        Some(wkb) => {
                            geometry_to_wkt(&wkb.to_wkb_object(), &mut wkt_builder)?;
                            wkt_builder.append_value("");
                        }
                        None => wkt_builder.append_null(),
                    }
                }
            }
            GeoDataType::LargeWKB => {
                for item in self.as_large_wkb().iter() {
                    match item {
                        Some(wkb) => {
                            geometry_to_wkt(&wkb.to_wkb_object(), &mut wkt_builder)?;
                            wkt_builder.append_value("");
                        }
                        None => wkt_builder.append_null(),
                    }
                }
            }
            GeoDataType::Rect(_dimension) => todo!(),
        }

        Ok(wkt_builder.finish())
    }
}

/// Create geometry to WKT representation.
pub fn geometry_to_wkt<W: Write>(
    geometry: &impl GeometryTrait<T = f64>,
    writer: &mut W,
) -> Result<(), Error> {
    match geometry.as_type() {
        GeometryType::Point(point) => point_to_wkt(point, writer),
        GeometryType::LineString(linestring) => linestring_to_wkt(linestring, writer),
        GeometryType::Polygon(polygon) => polygon_to_wkt(polygon, writer),
        GeometryType::MultiPoint(multi_point) => multi_point_to_wkt(multi_point, writer),
        GeometryType::MultiLineString(mls) => multi_linestring_to_wkt(mls, writer),
        GeometryType::MultiPolygon(multi_polygon) => multi_polygon_to_wkt(multi_polygon, writer),
        GeometryType::GeometryCollection(gc) => geometry_collection_to_wkt(gc, writer),
        GeometryType::Rect(rect) => rect_to_wkt(rect, writer),
    }
}

pub fn point_to_wkt<W: Write>(
    point: &impl PointTrait<T = f64>,
    writer: &mut W,
) -> Result<(), Error> {
    writer.write_str("POINT")?;

    let x = point.x();
    let y = point.y();

    // handle NaN, may should hapen when reading
    if x.is_nan() && y.is_nan() {
        writer.write_str(" EMPTY")?;
        return Ok(());
    }

    if point.dim() == 3 {
        writer.write_str(" Z")?;
    }

    writer.write_str(" (")?;

    // x
    let mut buffer = ryu::Buffer::new();
    writer.write_str(buffer.format(x))?;

    writer.write_str(" ")?;

    // y
    let mut buffer = ryu::Buffer::new();
    writer.write_str(buffer.format(y))?;

    // z .. n
    for nth in 2..point.dim() {
        writer.write_str(" ")?;
        let mut buffer = ryu::Buffer::new();
        writer.write_str(buffer.format(point.nth_unchecked(nth)))?;
    }

    writer.write_str(")")?;

    Ok(())
}

pub fn linestring_to_wkt<W: Write>(
    linestring: &impl LineStringTrait<T = f64>,
    writer: &mut W,
) -> Result<(), Error> {
    writer.write_str("LINESTRING ")?;

    if linestring.dim() == 3 {
        writer.write_str("Z ")?;
    }

    if linestring.num_coords() != 0 {
        add_coords(writer, linestring.coords())?;
    } else {
        writer.write_str(" EMPTY")?;
    }

    Ok(())
}

pub fn polygon_to_wkt<W: Write>(
    polygon: &impl PolygonTrait<T = f64>,
    writer: &mut W,
) -> Result<(), Error> {
    writer.write_str("POLYGON")?;

    if polygon.dim() == 3 {
        writer.write_str(" Z")?;
    }

    if let Some(exterior) = polygon.exterior() {
        if exterior.num_coords() != 0 {
            writer.write_str(" (")?;
            add_coords(writer, exterior.coords())?;
        } else {
            writer.write_str(" EMPTY")?;
            return Ok(());
        }
    } else {
        writer.write_str(" EMPTY")?;
        return Ok(());
    };

    for interior in polygon.interiors() {
        writer.write_str(",")?;
        add_coords(writer, interior.coords())?;
    }

    writer.write_str("))")?;

    Ok(())
}

pub fn multi_point_to_wkt<W: Write>(
    multi_point: &impl MultiPointTrait<T = f64>,
    writer: &mut W,
) -> Result<(), Error> {
    writer.write_str("MULTIPOINT")?;

    if multi_point.dim() == 3 {
        writer.write_str(" Z")?;
    }

    let mut points = multi_point.points();

    if let Some(first) = points.next() {
        writer.write_str(" (")?;

        add_point(writer, first)?;

        for point in points {
            writer.write_str(",")?;
            add_point(writer, point)?;
        }

        writer.write_str(")")?;
    } else {
        writer.write_str(" EMPTY")?;
    }

    Ok(())
}

pub fn multi_linestring_to_wkt<W: Write>(
    multi_linestring: &impl MultiLineStringTrait<T = f64>,
    writer: &mut W,
) -> Result<(), Error> {
    writer.write_str("MULTILINESTRING")?;

    if multi_linestring.dim() == 3 {
        writer.write_str(" Z")?;
    }

    let mut lines = multi_linestring.lines();

    if let Some(linestring) = lines.next() {
        writer.write_str(" (")?;
        add_coords(writer, linestring.coords())?;

        for linestring in lines {
            writer.write_str(",")?;
            add_coords(writer, linestring.coords())?;
        }

        writer.write_str(")")?;
    } else {
        writer.write_str(" EMPTY")?;
    }

    Ok(())
}

pub fn multi_polygon_to_wkt<W: Write>(
    multi_polygon: &impl MultiPolygonTrait<T = f64>,
    writer: &mut W,
) -> Result<(), Error> {
    writer.write_str("MULTIPOLYGON")?;

    if multi_polygon.dim() == 3 {
        writer.write_str(" Z")?;
    }

    let mut polygons = multi_polygon.polygons();

    if let Some(polygon) = polygons.next() {
        writer.write_str(" ((")?;

        add_coords(writer, polygon.exterior().unwrap().coords())?;
        for interior in polygon.interiors() {
            writer.write_str(",")?;
            add_coords(writer, interior.coords())?;
        }

        for polygon in polygons {
            writer.write_str("),(")?;

            add_coords(writer, polygon.exterior().unwrap().coords())?;
            for interior in polygon.interiors() {
                writer.write_str(",")?;
                add_coords(writer, interior.coords())?;
            }
        }

        writer.write_str("))")?;
    } else {
        writer.write_str(" EMPTY")?;
    };

    Ok(())
}

pub fn geometry_collection_to_wkt<W: Write>(
    gc: &impl GeometryCollectionTrait<T = f64>,
    writer: &mut W,
) -> Result<(), Error> {
    writer.write_str("GEOMETRYCOLLECTION")?;

    if gc.dim() == 3 {
        writer.write_str(" Z")?;
    }

    let mut geometries = gc.geometries();

    if let Some(first) = geometries.next() {
        writer.write_str(" (")?;

        geometry_to_wkt(&first, writer)?;

        for geom in geometries {
            writer.write_str(",")?;
            geometry_to_wkt(&geom, writer)?;
        }

        writer.write_str(")")?;
    } else {
        writer.write_str(" EMPTY")?;
    }

    Ok(())
}

pub fn rect_to_wkt<W: Write>(rect: &impl RectTrait<T = f64>, writer: &mut W) -> Result<(), Error> {
    writer.write_str("POLYGON")?;
    let lower = rect.lower();
    let upper = rect.upper();

    match rect.dim() {
        2 => writer.write_fmt(format_args!(
            " ({0} {1},{2} {1},{2} {3},{0} {3},{0} {1})",
            lower.x(),
            lower.y(),
            upper.x(),
            upper.y(),
        ))?,
        3 => todo!("cube as polygon / linestring / multipoint?"),

        _ => unimplemented!(),
    };

    Ok(())
}

fn add_coord<W: Write, C: CoordTrait<T = f64>>(writer: &mut W, coord: C) -> Result<(), Error> {
    // x
    let mut buffer = ryu::Buffer::new();
    writer.write_str(buffer.format(coord.x()))?;

    writer.write_str(" ")?;

    // y
    let mut buffer = ryu::Buffer::new();
    writer.write_str(buffer.format(coord.y()))?;

    // z .. n
    for nth in 2..coord.dim() {
        writer.write_str(" ")?;
        let mut buffer = ryu::Buffer::new();
        writer.write_str(buffer.format(coord.nth_unchecked(nth)))?;
    }

    Ok(())
}

fn add_point<W: Write, P: PointTrait<T = f64>>(writer: &mut W, point: P) -> Result<(), Error> {
    writer.write_str("(")?;

    // x
    let mut buffer = ryu::Buffer::new();
    writer.write_str(buffer.format(point.x()))?;

    writer.write_str(" ")?;

    // y
    let mut buffer = ryu::Buffer::new();
    writer.write_str(buffer.format(point.y()))?;

    // z .. n
    for nth in 2..point.dim() {
        writer.write_str(" ")?;
        let mut buffer = ryu::Buffer::new();
        writer.write_str(buffer.format(point.nth_unchecked(nth)))?;
    }

    writer.write_str(")")?;

    Ok(())
}

fn add_coords<W: Write, C: CoordTrait<T = f64>>(
    writer: &mut W,
    mut coords: impl Iterator<Item = C>,
) -> Result<(), Error> {
    writer.write_str("(")?;

    let first = coords.next().unwrap();
    add_coord(writer, first)?;

    for coord in coords {
        writer.write_str(",")?;
        add_coord(writer, coord)?;
    }

    writer.write_str(")")?;

    Ok(())
}
