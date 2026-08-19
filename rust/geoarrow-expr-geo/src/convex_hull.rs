use std::sync::Arc;

use geo::algorithm::convex_hull::quick_hull_indices;
use geo_traits::GeometryTrait;
use geoarrow_array::array::PolygonArray;
use geoarrow_array::builder::{GeometryBuilder, PolygonBuilder};
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_schema::error::GeoArrowResult;
use geoarrow_schema::{CoordType, Dimension, GeometryType as ArrowGeometryType, PolygonType};

use crate::dim_geom::{DimPolygon, DimPolygonParts, exterior_coords};

pub fn convex_hull(
    array: &dyn GeoArrowArray,
    coord_type: CoordType,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    if let Some(dim) = array.data_type().dimension() {
        let result = downcast_geoarrow_array!(array, convex_hull_typed_impl, coord_type, dim)?;
        Ok(Arc::new(result))
    } else {
        downcast_geoarrow_array!(array, convex_hull_geometry_impl, coord_type)
    }
}

fn convex_hull_typed_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    coord_type: CoordType,
    dim: Dimension,
) -> GeoArrowResult<PolygonArray> {
    let typ =
        PolygonType::new(dim, array.data_type().metadata().clone()).with_coord_type(coord_type);
    let mut builder = PolygonBuilder::new(typ);

    for item in array.iter() {
        if let Some(geom) = item {
            let scalar = geom?;
            let hull = hull_parts(&scalar);
            builder.push_polygon(Some(&DimPolygon {
                dim: scalar.dim(),
                parts: &hull,
            }))?;
        } else {
            builder.push_polygon(None::<&geo::Polygon>.as_ref())?;
        }
    }

    Ok(builder.finish())
}

fn convex_hull_geometry_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    coord_type: CoordType,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    let geom_typ =
        ArrowGeometryType::new(array.data_type().metadata().clone()).with_coord_type(coord_type);
    let mut builder = GeometryBuilder::new(geom_typ);

    for item in array.iter() {
        if let Some(geom) = item {
            let scalar = geom?;
            let hull = hull_parts(&scalar);
            builder.push_geometry(Some(&DimPolygon {
                dim: scalar.dim(),
                parts: &hull,
            }))?;
        } else {
            builder.push_geometry(None::<&geo::Geometry>.as_ref())?;
        }
    }

    Ok(Arc::new(builder.finish()))
}

fn hull_parts<G: GeometryTrait<T = f64>>(geom: &G) -> DimPolygonParts {
    let coords = exterior_coords(geom);
    let xy: Vec<geo::Coord<f64>> = coords
        .iter()
        .map(|c| geo::Coord { x: c[0], y: c[1] })
        .collect();
    DimPolygonParts {
        exterior: quick_hull_indices(&xy).iter().map(|&i| coords[i]).collect(),
        interiors: Vec::new(),
    }
}

#[cfg(test)]
mod test {
    use geo_traits::Dimensions;
    use geoarrow_array::builder::RectBuilder;
    use geoarrow_array::cast::AsGeoArrowArray;
    use geoarrow_schema::{BoxType, CoordType, Dimension};

    use super::*;
    use crate::dim_geom::{DimCoord, all_coords};
    use crate::test_util::{NOTCH_SQUARE, polygon_coords, xyzm_polygon_array};

    #[test]
    fn geometry_array_preserves_zm() {
        let arr = geoarrow_array::test::geometry::array(CoordType::Separated, false);
        let out_arc = convex_hull(&arr, CoordType::Separated).unwrap();
        let out = out_arc.as_geometry();

        let mut checked = 0;
        for (input, output) in arr.iter().zip(out.iter()) {
            let (Some(Ok(in_g)), Some(Ok(out_g))) = (input, output) else {
                continue;
            };
            let in_coords = all_coords(&in_g);
            for v in all_coords(&out_g) {
                assert!(
                    in_coords.contains(&v),
                    "hull vertex {v:?} is not an input coordinate"
                );
                checked += 1;
            }
        }
        assert!(checked > 0, "the test examined no hull vertices");
    }

    #[test]
    fn typed_array_preserves_zm() {
        let arr = xyzm_polygon_array(&[(&NOTCH_SQUARE, &[])]);
        let out_arc = convex_hull(&arr, CoordType::Separated).unwrap();
        let out = out_arc.as_polygon();
        assert_eq!(out.data_type().dimension(), Some(Dimension::XYZM));

        let hull = all_coords(&out.value(0).unwrap());
        assert!(!hull.is_empty());
        for v in hull {
            assert!(
                NOTCH_SQUARE.contains(&v),
                "hull vertex {v:?} is not an input coordinate"
            );
        }
    }

    #[test]
    fn empty_polygon_gives_empty_hull() {
        let arr = xyzm_polygon_array(&[(&[], &[])]);
        let out_arc = convex_hull(&arr, CoordType::Separated).unwrap();
        let out = out_arc.as_polygon();
        assert!(polygon_coords(&out.value(0).unwrap()).is_empty());
    }

    /// The two corners that a rect does not store have no Z value of their own.
    #[test]
    fn rect_preserves_z() {
        let mut builder = RectBuilder::new(BoxType::new(Dimension::XYZ, Default::default()));
        let corner = |vals| DimCoord {
            dim: Dimensions::Xyz,
            vals,
        };
        builder.push_min_max(&corner([0.0, 0.0, 5.0, 0.0]), &corner([2.0, 3.0, 9.0, 0.0]));

        let out_arc = convex_hull(&builder.finish(), CoordType::Separated).unwrap();
        let out = out_arc.as_polygon();
        assert_eq!(out.data_type().dimension(), Some(Dimension::XYZ));

        let rings = polygon_coords(&out.value(0).unwrap());
        let zs: Vec<f64> = rings.concat().iter().map(|c| c[2]).collect();
        assert!(
            zs.iter().all(|&z| z == 5.0 || z == 9.0),
            "the rect hull changed a Z value: {zs:?}"
        );
        assert!(
            zs.contains(&9.0),
            "the hull must keep the Z value of the maximum corner"
        );
    }
}
