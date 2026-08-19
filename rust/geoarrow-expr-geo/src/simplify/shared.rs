use std::sync::Arc;

use geo::PolygonIndices;
use geo_traits::to_geo::{ToGeoLineString, ToGeoPolygon};
use geo_traits::{
    GeometryTrait, GeometryType as GtType, LineStringTrait, MultiLineStringTrait,
    MultiPolygonTrait, PolygonTrait,
};
use geoarrow_array::array::{
    LineStringArray, MultiLineStringArray, MultiPolygonArray, PolygonArray,
};
use geoarrow_array::builder::{
    GeometryBuilder, LineStringBuilder, MultiLineStringBuilder, MultiPolygonBuilder, PolygonBuilder,
};
use geoarrow_array::cast::AsGeoArrowArray;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, IntoArrow, downcast_geoarrow_array};
use geoarrow_schema::error::GeoArrowResult;
use geoarrow_schema::{GeoArrowType, GeometryType};

use crate::dim_geom::{
    DimMultiLineString, DimMultiPolygon, DimPolygon, DimPolygonParts, DimRing, Ordinates, ordinates,
};
use crate::util::copy_geoarrow_array_ref;

/// Returns the indices of the coordinates to keep in one ring.
pub(crate) type RingSelector = dyn Fn(&geo::LineString<f64>, f64) -> Vec<usize>;

pub(crate) type PolygonSelector = dyn Fn(&geo::Polygon<f64>, f64) -> PolygonIndices;

pub(crate) fn simplify_with(
    array: &dyn GeoArrowArray,
    epsilon: f64,
    select: &RingSelector,
    poly_select: &PolygonSelector,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    use GeoArrowType::*;
    match array.data_type() {
        Point(_) | MultiPoint(_) | GeometryCollection(_) | Rect(_) => {
            Ok(copy_geoarrow_array_ref(array))
        }
        LineString(_) => linestring(array.as_line_string(), epsilon, select),
        Polygon(_) => polygon(array.as_polygon(), epsilon, poly_select),
        MultiLineString(_) => multi_linestring(array.as_multi_line_string(), epsilon, select),
        MultiPolygon(_) => multi_polygon(array.as_multi_polygon(), epsilon, poly_select),
        _ => downcast_geoarrow_array!(array, geometry_impl, epsilon, select, poly_select),
    }
}

fn linestring(
    array: &LineStringArray,
    epsilon: f64,
    select: &RingSelector,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    let mut builder = LineStringBuilder::new(array.extension_type().clone());
    for item in array.iter() {
        if let Some(geom) = item {
            let scalar = geom?;
            let coords = select_ring(&scalar, epsilon, select);
            builder.push_line_string(Some(&DimRing {
                dim: scalar.dim(),
                coords: &coords,
            }))?;
        } else {
            builder.push_line_string(None::<&geo::LineString>.as_ref())?;
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn polygon(
    array: &PolygonArray,
    epsilon: f64,
    poly_select: &PolygonSelector,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    let mut builder = PolygonBuilder::new(array.extension_type().clone());
    for item in array.iter() {
        if let Some(geom) = item {
            let scalar = geom?;
            let parts = select_polygon(&scalar, epsilon, poly_select);
            builder.push_polygon(Some(&DimPolygon {
                dim: scalar.dim(),
                parts: &parts,
            }))?;
        } else {
            builder.push_polygon(None::<&geo::Polygon>.as_ref())?;
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn multi_linestring(
    array: &MultiLineStringArray,
    epsilon: f64,
    select: &RingSelector,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    let mut builder = MultiLineStringBuilder::new(array.extension_type().clone());
    for item in array.iter() {
        if let Some(geom) = item {
            let scalar = geom?;
            let mls = select_multi_linestring(&scalar, epsilon, select);
            builder.push_multi_line_string(Some(&mls))?;
        } else {
            builder.push_multi_line_string(None::<&geo::MultiLineString>.as_ref())?;
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn multi_polygon(
    array: &MultiPolygonArray,
    epsilon: f64,
    poly_select: &PolygonSelector,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    let mut builder = MultiPolygonBuilder::new(array.extension_type().clone());
    for item in array.iter() {
        if let Some(geom) = item {
            let scalar = geom?;
            let mp = select_multi_polygon(&scalar, epsilon, poly_select);
            builder.push_multi_polygon(Some(&mp))?;
        } else {
            builder.push_multi_polygon(None::<&geo::MultiPolygon>.as_ref())?;
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn geometry_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    epsilon: f64,
    select: &RingSelector,
    poly_select: &PolygonSelector,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    let geom_typ = GeometryType::new(array.data_type().metadata().clone());
    let mut builder = GeometryBuilder::new(geom_typ);
    for item in array.iter() {
        if let Some(geom) = item {
            let scalar = geom?;
            match scalar.as_type() {
                GtType::LineString(ls) => {
                    let coords = select_ring(ls, epsilon, select);
                    builder.push_geometry(Some(&DimRing {
                        dim: ls.dim(),
                        coords: &coords,
                    }))?;
                }
                GtType::Polygon(p) => {
                    let parts = select_polygon(p, epsilon, poly_select);
                    builder.push_geometry(Some(&DimPolygon {
                        dim: p.dim(),
                        parts: &parts,
                    }))?;
                }
                GtType::MultiLineString(mls) => {
                    builder.push_geometry(Some(&select_multi_linestring(mls, epsilon, select)))?;
                }
                GtType::MultiPolygon(mp) => {
                    builder.push_geometry(Some(&select_multi_polygon(mp, epsilon, poly_select)))?;
                }
                _ => {
                    builder.push_geometry(Some(&scalar))?;
                }
            }
        } else {
            builder.push_geometry(None::<&geo::Geometry>.as_ref())?;
        }
    }
    Ok(Arc::new(builder.finish()))
}

/// `geo` returns indices into a copy of the ring, thus each index must be valid.
fn ring_coord<L: LineStringTrait<T = f64>>(ring: &L, i: usize) -> Ordinates {
    ordinates(&ring.coord(i).expect("geo returned an invalid index"))
}

fn select_ring<L: LineStringTrait<T = f64>>(
    ring: &L,
    epsilon: f64,
    select: &RingSelector,
) -> Vec<Ordinates> {
    select(&ring.to_line_string(), epsilon)
        .iter()
        .map(|&i| ring_coord(ring, i))
        .collect()
}

/// `to_polygon` closes an open ring, thus `geo` can return an index one past the last
/// one. That index is the coordinate that closes the ring, which is the first one.
fn closed_ring_coord<L: LineStringTrait<T = f64>>(ring: &L, i: usize) -> Ordinates {
    ring_coord(ring, if i == ring.num_coords() { 0 } else { i })
}

fn select_polygon<P: PolygonTrait<T = f64>>(
    poly: &P,
    epsilon: f64,
    poly_select: &PolygonSelector,
) -> DimPolygonParts {
    let Some(exterior_ring) = poly.exterior() else {
        return DimPolygonParts::default();
    };
    let indices = poly_select(&poly.to_polygon(), epsilon);
    DimPolygonParts {
        exterior: indices
            .exterior()
            .iter()
            .map(|&i| closed_ring_coord(&exterior_ring, i))
            .collect(),
        interiors: indices
            .interiors()
            .iter()
            .zip(poly.interiors())
            .map(|(kept, ring)| kept.iter().map(|&i| closed_ring_coord(&ring, i)).collect())
            .collect(),
    }
}

fn select_multi_linestring<M: MultiLineStringTrait<T = f64>>(
    mls: &M,
    epsilon: f64,
    select: &RingSelector,
) -> DimMultiLineString {
    DimMultiLineString {
        dim: mls.dim(),
        line_strings: mls
            .line_strings()
            .map(|ls| select_ring(&ls, epsilon, select))
            .collect(),
    }
}

fn select_multi_polygon<M: MultiPolygonTrait<T = f64>>(
    mp: &M,
    epsilon: f64,
    poly_select: &PolygonSelector,
) -> DimMultiPolygon {
    DimMultiPolygon {
        dim: mp.dim(),
        polygons: mp
            .polygons()
            .map(|poly| select_polygon(&poly, epsilon, poly_select))
            .collect(),
    }
}
