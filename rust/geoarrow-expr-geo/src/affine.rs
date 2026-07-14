use std::sync::Arc;

use geo::{AffineTransform, Coord};
use geo_traits::GeometryTrait;
use geoarrow_array::builder::{
    GeometryBuilder, GeometryCollectionBuilder, LineStringBuilder, MultiLineStringBuilder,
    MultiPointBuilder, MultiPolygonBuilder, PointBuilder, PolygonBuilder,
};
use geoarrow_array::cast::AsGeoArrowArray;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_schema::error::GeoArrowResult;
use geoarrow_schema::{
    CoordType, GeoArrowType, GeometryCollectionType, GeometryType as ArrowGeometryType,
    LineStringType, MultiLineStringType, MultiPointType, MultiPolygonType, PointType, PolygonType,
};

use crate::dim_geom::{DimGeometry, Ordinates, map_ordinates};

/// Rotate each geometry counter clockwise about `(origin_x, origin_y)`.
pub fn rotate(
    array: &dyn GeoArrowArray,
    degrees: f64,
    origin_x: f64,
    origin_y: f64,
    coord_type: CoordType,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    apply_affine(
        array,
        &AffineTransform::rotate(degrees, (origin_x, origin_y)),
        coord_type,
    )
}

/// Scale each geometry about `(origin_x, origin_y)`.
pub fn scale(
    array: &dyn GeoArrowArray,
    x_factor: f64,
    y_factor: f64,
    origin_x: f64,
    origin_y: f64,
    coord_type: CoordType,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    apply_affine(
        array,
        &AffineTransform::scale(x_factor, y_factor, (origin_x, origin_y)),
        coord_type,
    )
}

/// Skew each geometry about `(origin_x, origin_y)`.
pub fn skew(
    array: &dyn GeoArrowArray,
    x_degrees: f64,
    y_degrees: f64,
    origin_x: f64,
    origin_y: f64,
    coord_type: CoordType,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    apply_affine(
        array,
        &AffineTransform::skew(x_degrees, y_degrees, (origin_x, origin_y)),
        coord_type,
    )
}

/// Translate each geometry by `(x_offset, y_offset)`.
pub fn translate(
    array: &dyn GeoArrowArray,
    x_offset: f64,
    y_offset: f64,
    coord_type: CoordType,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    apply_affine(
        array,
        &AffineTransform::translate(x_offset, y_offset),
        coord_type,
    )
}

/// Transform every geometry of an array. The transform reads x and y only, thus
/// Z and M pass through and the output keeps the dimension of the input.
///
/// An array with a builder of its own keeps its geometry type. Every other
/// encoding (WKB, WKT, a mixed `Geometry` array) gives a `Geometry` array, in
/// which a `Rect` or `Triangle` becomes a polygon and a `Line` a line string.
fn apply_affine(
    array: &dyn GeoArrowArray,
    transform: &AffineTransform,
    coord_type: CoordType,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    /// Every builder of a concrete geometry type accepts a whole geometry, thus
    /// one loop serves them all. Only the output type differs.
    macro_rules! affine_typed {
        ($array:expr, $type_ctor:ty, $builder:ty) => {{
            let array = $array;
            let dim = array
                .data_type()
                .dimension()
                .expect("a typed array has a dimension");
            let typ = <$type_ctor>::new(dim, array.data_type().metadata().clone())
                .with_coord_type(coord_type);
            let mut builder = <$builder>::new(typ);
            for item in array.iter() {
                let out = match item {
                    Some(geom) => Some(transform_geometry(&geom?, transform)),
                    None => None,
                };
                builder.push_geometry(out.as_ref())?;
            }
            Ok(Arc::new(builder.finish()))
        }};
    }

    use GeoArrowType::*;
    match array.data_type() {
        Point(_) => affine_typed!(array.as_point(), PointType, PointBuilder),
        LineString(_) => affine_typed!(array.as_line_string(), LineStringType, LineStringBuilder),
        Polygon(_) => affine_typed!(array.as_polygon(), PolygonType, PolygonBuilder),
        MultiPoint(_) => affine_typed!(array.as_multi_point(), MultiPointType, MultiPointBuilder),
        MultiLineString(_) => affine_typed!(
            array.as_multi_line_string(),
            MultiLineStringType,
            MultiLineStringBuilder
        ),
        MultiPolygon(_) => affine_typed!(
            array.as_multi_polygon(),
            MultiPolygonType,
            MultiPolygonBuilder
        ),
        GeometryCollection(_) => affine_typed!(
            array.as_geometry_collection(),
            GeometryCollectionType,
            GeometryCollectionBuilder
        ),
        _ => downcast_geoarrow_array!(array, affine_geometry_impl, transform, coord_type),
    }
}

/// The transform applies to x and y. Z and M carry through unchanged.
fn transform_geometry<G: GeometryTrait<T = f64>>(
    g: &G,
    transform: &AffineTransform,
) -> DimGeometry {
    map_ordinates(g, &|mut vals: Ordinates| {
        let out = transform.apply(Coord {
            x: vals[0],
            y: vals[1],
        });
        vals[0] = out.x;
        vals[1] = out.y;
        vals
    })
}

fn affine_geometry_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    transform: &AffineTransform,
    coord_type: CoordType,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    let typ =
        ArrowGeometryType::new(array.data_type().metadata().clone()).with_coord_type(coord_type);
    let mut builder = GeometryBuilder::new(typ);

    for item in array.iter() {
        if let Some(geom) = item {
            builder.push_geometry(Some(&transform_geometry(&geom?, transform)))?;
        } else {
            builder.push_geometry(None::<&DimGeometry>)?;
        }
    }

    Ok(Arc::new(builder.finish()))
}

#[cfg(test)]
mod test {
    use geo::{AffineOps, polygon};
    use geo_traits::to_geo::ToGeoPolygon;
    use geo_traits::{Dimensions, PointTrait};
    use geoarrow_array::builder::{PolygonBuilder, WkbBuilder};
    use geoarrow_schema::{Dimension, WkbType};

    use super::*;
    use crate::dim_geom::{DimLineStringBuf, DimPoint, all_coords};
    use crate::test_util::{
        NEAR_COLLINEAR_LS, PLAIN_SQUARE, array_coords, xyzm_linestring_array,
        xyzm_multilinestring_array, xyzm_multipolygon_array, xyzm_polygon_array,
    };

    const DX: f64 = 10.0;
    const DY: f64 = 20.0;

    fn shift(coords: Vec<Ordinates>) -> Vec<Ordinates> {
        coords
            .into_iter()
            .map(|c| [c[0] + DX, c[1] + DY, c[2], c[3]])
            .collect()
    }

    fn polygon_array(poly: &geo::Polygon) -> geoarrow_array::array::PolygonArray {
        let typ = PolygonType::new(Dimension::XY, Default::default())
            .with_coord_type(CoordType::Interleaved);
        let mut builder = PolygonBuilder::new(typ);
        builder.push_polygon(Some(poly)).unwrap();
        builder.finish()
    }

    fn first_polygon(array: &Arc<dyn GeoArrowArray>) -> geo::Polygon {
        array
            .as_polygon()
            .iter()
            .next()
            .unwrap()
            .unwrap()
            .unwrap()
            .to_polygon()
    }

    /// Each public function must reach for its own `AffineTransform`. `geo`
    /// applying the same transform is the oracle for a swap of one for another.
    #[test]
    fn the_four_transforms_match_geo() {
        let square = polygon![(x: 0., y: 0.), (x: 2., y: 0.), (x: 2., y: 2.), (x: 0., y: 2.)];
        let arr = polygon_array(&square);
        let ct = CoordType::Interleaved;

        let cases = [
            (
                rotate(&arr, 30.0, 1.0, 1.0, ct).unwrap(),
                AffineTransform::rotate(30.0, (1.0, 1.0)),
            ),
            (
                scale(&arr, 2.0, 3.0, 1.0, 1.0, ct).unwrap(),
                AffineTransform::scale(2.0, 3.0, (1.0, 1.0)),
            ),
            (
                skew(&arr, 30.0, 15.0, 0.0, 0.0, ct).unwrap(),
                AffineTransform::skew(30.0, 15.0, (0.0, 0.0)),
            ),
            (
                translate(&arr, 10.0, 20.0, ct).unwrap(),
                AffineTransform::translate(10.0, 20.0),
            ),
        ];

        for (result, transform) in cases {
            assert_eq!(first_polygon(&result), square.affine_transform(&transform));
        }
    }

    /// The transform reads x and y only, thus Z and M reach the output unchanged.
    /// Each of these arrays has a builder of its own, thus it keeps its type.
    #[test]
    fn a_typed_array_keeps_its_type_and_its_z_and_m() {
        let square = (&PLAIN_SQUARE[..], &[][..]);
        let arrays: [Arc<dyn GeoArrowArray>; 5] = [
            Arc::new(xyzm_linestring_array(&[&NEAR_COLLINEAR_LS])),
            Arc::new(xyzm_polygon_array(&[square])),
            Arc::new(xyzm_multilinestring_array(&[&[&NEAR_COLLINEAR_LS]])),
            Arc::new(xyzm_multipolygon_array(&[&[square]])),
            Arc::new(geoarrow_array::test::geometrycollection::array(
                CoordType::Separated,
                Dimension::XYZ,
                false,
            )),
        ];

        for arr in arrays {
            let result = translate(&*arr, DX, DY, CoordType::Separated).unwrap();
            assert_eq!(result.data_type(), arr.data_type());
            assert_eq!(array_coords(&*result), shift(array_coords(&*arr)));
        }
    }

    /// An empty point has no coordinate to transform, thus it stays an empty
    /// point instead of a panic.
    #[test]
    fn xyz_point_array_keeps_z_and_empty_points() {
        let typ = PointType::new(Dimension::XYZ, Default::default())
            .with_coord_type(CoordType::Separated);
        let mut builder = PointBuilder::new(typ);
        builder.push_point(Some(&DimPoint {
            dim: Dimensions::Xyz,
            coord: Some([1.0, 2.0, 3.0, 0.0]),
        }));
        builder.push_empty();
        builder.push_null();

        let result = translate(&builder.finish(), DX, DY, CoordType::Separated).unwrap();
        assert_eq!(result.data_type().dimension(), Some(Dimension::XYZ));

        let points = result.as_point();
        let mut rows = points.iter();
        let first = rows.next().unwrap().unwrap().unwrap();
        assert_eq!(all_coords(&first), [[11.0, 22.0, 3.0, 0.0]]);
        assert!(rows.next().unwrap().unwrap().unwrap().coord().is_none());
        assert!(rows.next().unwrap().is_none());
    }

    /// WKB has no builder of its own, thus it takes the `Geometry` fallback. That
    /// path must carry Z as well.
    #[test]
    fn xyz_wkb_input_keeps_z_and_nulls() {
        let geom = DimGeometry::LineString(DimLineStringBuf {
            dim: Dimensions::Xyz,
            coords: vec![[0.0, 0.0, 7.0, 0.0], [2.0, 4.0, 9.0, 0.0]],
        });
        let mut builder: WkbBuilder<i32> = WkbBuilder::new(WkbType::new(Default::default()));
        builder.push_geometry(Some(&geom)).unwrap();
        builder.push_geometry(None::<&DimGeometry>).unwrap();

        let result = translate(&builder.finish(), DX, DY, CoordType::Separated).unwrap();
        assert_eq!(
            array_coords(&*result),
            [[10.0, 20.0, 7.0, 0.0], [12.0, 24.0, 9.0, 0.0]]
        );
        assert!(result.as_geometry().iter().nth(1).unwrap().is_none());
    }

    /// A rect array has no builder of its own either. Each rect becomes a polygon
    /// in the ring order of `geo::Rect::to_polygon`.
    #[test]
    fn rect_array_lowers_to_a_polygon() {
        use geo::{Rect, coord};
        use geoarrow_array::builder::RectBuilder;
        use geoarrow_schema::BoxType;

        let mut builder = RectBuilder::new(BoxType::new(Dimension::XY, Default::default()));
        builder.push_rect(Some(&Rect::new(
            coord! { x: 10., y: 10. },
            coord! { x: 30., y: 20. },
        )));

        let result = translate(&builder.finish(), 100.0, 200.0, CoordType::Interleaved).unwrap();
        assert_eq!(
            array_coords(&*result),
            [
                [130.0, 210.0, 0.0, 0.0],
                [130.0, 220.0, 0.0, 0.0],
                [110.0, 220.0, 0.0, 0.0],
                [110.0, 210.0, 0.0, 0.0],
                [130.0, 210.0, 0.0, 0.0],
            ]
        );
    }

    /// No GeoArrow array holds a triangle or a line, thus these two arms of
    /// `transform_geometry` need `geo` types to reach them.
    #[test]
    fn a_triangle_and_a_line_lower_to_a_polygon_and_a_line_string() {
        use geo::{Line, Triangle, coord};
        let transform = AffineTransform::translate(1.0, 2.0);

        let triangle = Triangle::new(
            coord! { x: 0., y: 0. },
            coord! { x: 4., y: 0. },
            coord! { x: 0., y: 3. },
        );
        let lowered = transform_geometry(&triangle, &transform);
        assert!(matches!(lowered, DimGeometry::Polygon(_)));
        assert_eq!(
            all_coords(&lowered),
            [
                [1.0, 2.0, 0.0, 0.0],
                [5.0, 2.0, 0.0, 0.0],
                [1.0, 5.0, 0.0, 0.0],
                [1.0, 2.0, 0.0, 0.0],
            ]
        );

        let line = Line::new(coord! { x: 0., y: 0. }, coord! { x: 2., y: 4. });
        let lowered = transform_geometry(&line, &transform);
        assert!(matches!(lowered, DimGeometry::LineString(_)));
        assert_eq!(
            all_coords(&lowered),
            [[1.0, 2.0, 0.0, 0.0], [3.0, 6.0, 0.0, 0.0]]
        );
    }
}
