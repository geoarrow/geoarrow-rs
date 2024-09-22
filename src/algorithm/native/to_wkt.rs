use crate::geo_traits::*;

/// Converts the input to WKT representation.
pub trait ToWKT {
    fn to_wkt(&self) -> String;
}

impl<G: GeometryTrait<T = f64>> ToWKT for G {
    fn to_wkt(&self) -> String {
        match self.as_type() {
            GeometryType::Point(point) => {
                let mut wkt = String::from("POINT");

                let x = point.x();
                let y = point.y();

                // handle NaN, may should hapen when reading
                if x.is_nan() && y.is_nan() {
                    wkt.push_str(" EMPTY");
                    return wkt;
                }

                if point.dim() == 3 {
                    wkt.push('Z')
                }

                wkt.push('(');

                // x
                let mut buffer = ryu::Buffer::new();
                wkt.push_str(buffer.format(x));

                wkt.push(' ');

                // y
                let mut buffer = ryu::Buffer::new();
                wkt.push_str(buffer.format(y));

                // z .. n
                for nth in 2..point.dim() {
                    wkt.push(' ');
                    let mut buffer = ryu::Buffer::new();
                    wkt.push_str(buffer.format(point.nth_unchecked(nth)));
                }

                wkt.push(')');

                wkt
            }
            GeometryType::LineString(line_string) => {
                let mut wkt = String::from("LINESTRING");

                if line_string.dim() == 3 {
                    wkt.push('Z')
                }

                if line_string.num_coords() != 0 {
                    add_coords(&mut wkt, line_string.coords());
                } else {
                    wkt.push_str(" EMPTY");
                }

                wkt
            }
            GeometryType::Polygon(polygon) => {
                let mut wkt = String::from("POLYGON");

                if polygon.dim() == 3 {
                    wkt.push('Z')
                }

                if let Some(exterior) = polygon.exterior() {
                    if exterior.num_coords() != 0 {
                        wkt.push('(');
                        add_coords(&mut wkt, exterior.coords());
                    } else {
                        wkt.push_str(" EMPTY");
                        return wkt;
                    }
                } else {
                    wkt.push_str(" EMPTY");
                    return wkt;
                };

                for interior in polygon.interiors() {
                    wkt.push(',');
                    add_coords(&mut wkt, interior.coords());
                }

                wkt.push_str("))");

                wkt
            }
            GeometryType::MultiPoint(multi_point) => {
                let mut wkt = String::from("MULTIPOINT");

                if multi_point.dim() == 3 {
                    wkt.push('Z')
                }

                let mut points = multi_point.points();

                if let Some(first) = points.next() {
                    wkt.push('(');

                    add_point(&mut wkt, first);

                    for point in points {
                        wkt.push(',');
                        add_point(&mut wkt, point);
                    }

                    wkt.push(')');
                } else {
                    wkt.push_str(" EMPTY");
                }

                wkt
            }
            GeometryType::MultiLineString(multi_line_string) => {
                let mut wkt = String::from("MULTILINESTRING");

                if multi_line_string.dim() == 3 {
                    wkt.push('Z')
                }

                let mut lines = multi_line_string.lines();

                if let Some(line_string) = lines.next() {
                    wkt.push('(');
                    add_coords(&mut wkt, line_string.coords());

                    for line_string in lines {
                        wkt.push(',');
                        add_coords(&mut wkt, line_string.coords());
                    }

                    wkt.push(')');
                } else {
                    wkt.push_str(" EMPTY");
                }

                wkt
            }
            GeometryType::MultiPolygon(multi_polygon) => {
                let mut wkt = String::from("MULTIPOLYGON");

                if multi_polygon.dim() == 3 {
                    wkt.push('Z')
                }

                let mut polygons = multi_polygon.polygons();

                if let Some(polygon) = polygons.next() {
                    wkt.push_str("((");

                    add_coords(&mut wkt, polygon.exterior().unwrap().coords());
                    for interior in polygon.interiors() {
                        wkt.push(',');
                        add_coords(&mut wkt, interior.coords());
                    }

                    for polygon in polygons {
                        wkt.push_str("),(");

                        add_coords(&mut wkt, polygon.exterior().unwrap().coords());
                        for interior in polygon.interiors() {
                            wkt.push(',');
                            add_coords(&mut wkt, interior.coords());
                        }
                    }

                    wkt.push_str("))");
                } else {
                    wkt.push_str("EMPTY");
                    return wkt;
                };

                wkt
            }
            GeometryType::GeometryCollection(gc) => {
                let mut wkt = String::from("GEOMETRYCOLLECTION");

                if gc.dim() == 3 {
                    wkt.push('Z')
                }

                let mut geometries = gc.geometries();

                if let Some(first) = geometries.next() {
                    wkt.push('(');

                    wkt.push_str(&first.to_wkt());

                    for geom in geometries {
                        wkt.push(',');
                        wkt.push_str(&geom.to_wkt());
                    }

                    wkt.push(')');
                } else {
                    wkt.push_str("EMPTY");
                }

                wkt
            }
            GeometryType::Rect(rect) => {
                let lower = rect.lower();
                let upper = rect.upper();

                match rect.dim() {
                    2 => format!(
                        "POLYGON({0} {1},{2} {1},{2} {3},{0} {3},{0} {1})",
                        lower.x(),
                        lower.y(),
                        upper.x(),
                        upper.y(),
                    ),
                    3 => todo!("cube as polygon / linestring / multipoint?"),

                    _ => unimplemented!(),
                }
            }
        }
    }
}

fn add_coord<C: CoordTrait<T = f64>>(wkt: &mut String, coord: C) {
    // x
    let mut buffer = ryu::Buffer::new();
    wkt.push_str(buffer.format(coord.x()));

    wkt.push(' ');

    // y
    let mut buffer = ryu::Buffer::new();
    wkt.push_str(buffer.format(coord.y()));

    // z .. n
    for nth in 2..coord.dim() {
        wkt.push(' ');
        let mut buffer = ryu::Buffer::new();
        wkt.push_str(buffer.format(coord.nth_unchecked(nth)));
    }
}

fn add_point<P: PointTrait<T = f64>>(wkt: &mut String, point: P) {
    wkt.push('(');

    // x
    let mut buffer = ryu::Buffer::new();
    wkt.push_str(buffer.format(point.x()));

    wkt.push(' ');

    // y
    let mut buffer = ryu::Buffer::new();
    wkt.push_str(buffer.format(point.y()));

    // z .. n
    for nth in 2..point.dim() {
        wkt.push(' ');
        let mut buffer = ryu::Buffer::new();
        wkt.push_str(buffer.format(point.nth_unchecked(nth)));
    }

    wkt.push(')');
}

fn add_coords<C: CoordTrait<T = f64>>(wkt: &mut String, mut coords: impl Iterator<Item = C>) {
    wkt.push('(');

    let first = coords.next().unwrap();
    add_coord(wkt, first);

    for coord in coords {
        wkt.push(',');
        add_coord(wkt, coord);
    }

    wkt.push(')');
}
