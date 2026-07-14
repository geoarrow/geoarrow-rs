//! XYZM test data. Each coordinate is `[x, y, z, m]`. Each vertex has different Z and
//! M values. A test can then identify the input coordinate for each output coordinate.

use geo_traits::to_geo::ToGeoPolygon;
use geo_traits::{Dimensions, LineStringTrait, PolygonTrait};
use geoarrow_array::array::{
    GeometryCollectionArray, LineStringArray, MultiLineStringArray, MultiPolygonArray, PolygonArray,
};
use geoarrow_array::builder::{
    GeometryBuilder, GeometryCollectionBuilder, LineStringBuilder, MultiLineStringBuilder,
    MultiPolygonBuilder, PolygonBuilder,
};
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_schema::{
    CoordType, Dimension, GeometryCollectionType, GeometryType, LineStringType,
    MultiLineStringType, MultiPolygonType, PolygonType,
};

use crate::dim_geom::{
    DimMultiLineString, DimMultiPolygon, DimPolygon, DimPolygonParts, DimRing, all_coords,
    ordinates,
};
use crate::util::to_geo::geometry_to_geo;

/// An almost straight line. Coordinate 1 is `0.1` from the straight line. Simplify
/// removes coordinate 1 at `NEAR_COLLINEAR_EPS`.
pub(crate) const NEAR_COLLINEAR_LS: [[f64; 4]; 4] = [
    [0.0, 0.0, 10.0, 100.0],
    [1.0, 0.1, 11.0, 101.0],
    [2.0, 0.0, 12.0, 102.0],
    [3.0, 3.0, 13.0, 103.0],
];
pub(crate) const NEAR_COLLINEAR_EPS: f64 = 0.15;
pub(crate) const NEAR_COLLINEAR_KEPT: [[f64; 4]; 3] = [
    NEAR_COLLINEAR_LS[0],
    NEAR_COLLINEAR_LS[2],
    NEAR_COLLINEAR_LS[3],
];

/// A closed square with a small notch at coordinate 1. Simplify removes the notch at
/// `NOTCH_EPS`.
pub(crate) const NOTCH_SQUARE: [[f64; 4]; 6] = [
    [0.0, 0.0, 20.0, 200.0],
    [5.0, 0.1, 21.0, 201.0],
    [10.0, 0.0, 22.0, 202.0],
    [10.0, 10.0, 23.0, 203.0],
    [0.0, 10.0, 24.0, 204.0],
    [0.0, 0.0, 20.0, 200.0],
];
pub(crate) const NOTCH_EPS: f64 = 0.7;
pub(crate) const NOTCH_SQUARE_KEPT: [[f64; 4]; 5] = [
    NOTCH_SQUARE[0],
    NOTCH_SQUARE[2],
    NOTCH_SQUARE[3],
    NOTCH_SQUARE[4],
    NOTCH_SQUARE[5],
];

/// A closed square at a distance from the other test data. Simplify removes no
/// coordinates from this square.
pub(crate) const PLAIN_SQUARE: [[f64; 4]; 5] = [
    [100.0, 100.0, 40.0, 400.0],
    [110.0, 100.0, 41.0, 401.0],
    [110.0, 110.0, 42.0, 402.0],
    [100.0, 110.0, 43.0, 403.0],
    [100.0, 100.0, 40.0, 400.0],
];

pub(crate) type Ring<'a> = &'a [[f64; 4]];

/// Exterior ring, then interior rings.
pub(crate) type PolygonRings<'a> = (Ring<'a>, &'a [Ring<'a>]);

fn parts((exterior, interiors): &PolygonRings) -> DimPolygonParts {
    DimPolygonParts {
        exterior: exterior.to_vec(),
        interiors: interiors.iter().map(|r| r.to_vec()).collect(),
    }
}

pub(crate) fn xyzm_linestring_array(rows: &[Ring<'_>]) -> LineStringArray {
    let typ = LineStringType::new(Dimension::XYZM, Default::default())
        .with_coord_type(CoordType::Separated);
    let mut b = LineStringBuilder::new(typ);
    for row in rows {
        b.push_line_string(Some(&DimRing {
            dim: Dimensions::Xyzm,
            coords: row,
        }))
        .unwrap();
    }
    b.finish()
}

pub(crate) fn xyzm_polygon_array(rows: &[PolygonRings<'_>]) -> PolygonArray {
    let typ =
        PolygonType::new(Dimension::XYZM, Default::default()).with_coord_type(CoordType::Separated);
    let mut b = PolygonBuilder::new(typ);
    for row in rows {
        let parts = parts(row);
        b.push_polygon(Some(&DimPolygon {
            dim: Dimensions::Xyzm,
            parts: &parts,
        }))
        .unwrap();
    }
    b.finish()
}

pub(crate) fn xyzm_multilinestring_array(rows: &[&[Ring<'_>]]) -> MultiLineStringArray {
    let typ = MultiLineStringType::new(Dimension::XYZM, Default::default())
        .with_coord_type(CoordType::Separated);
    let mut b = MultiLineStringBuilder::new(typ);
    for row in rows {
        b.push_multi_line_string(Some(&DimMultiLineString {
            dim: Dimensions::Xyzm,
            line_strings: row.iter().map(|r| r.to_vec()).collect(),
        }))
        .unwrap();
    }
    b.finish()
}

pub(crate) fn xyzm_multipolygon_array(rows: &[&[PolygonRings<'_>]]) -> MultiPolygonArray {
    let typ = MultiPolygonType::new(Dimension::XYZM, Default::default())
        .with_coord_type(CoordType::Separated);
    let mut b = MultiPolygonBuilder::new(typ);
    for row in rows {
        b.push_multi_polygon(Some(&DimMultiPolygon {
            dim: Dimensions::Xyzm,
            polygons: row.iter().map(parts).collect(),
        }))
        .unwrap();
    }
    b.finish()
}

/// A one row collection array, for the kernels that must recurse into a
/// collection because `geo` does not.
pub(crate) fn geometry_collection_array(gc: &geo::GeometryCollection) -> GeometryCollectionArray {
    let typ = GeometryCollectionType::new(Dimension::XY, Default::default())
        .with_coord_type(CoordType::Interleaved);
    let mut b = GeometryCollectionBuilder::new(typ);
    b.push_geometry_collection(Some(gc)).unwrap();
    b.finish()
}

/// Every coordinate of every present row, in row order.
pub(crate) fn array_coords(array: &dyn GeoArrowArray) -> Vec<[f64; 4]> {
    downcast_geoarrow_array!(array, collect_coords)
}

fn collect_coords<'a>(array: &'a impl GeoArrowArrayAccessor<'a>) -> Vec<[f64; 4]> {
    array
        .iter()
        .flatten()
        .flat_map(|geom| all_coords(&geom.unwrap()))
        .collect()
}

/// Exterior ring first, then the interiors. An empty polygon gives no rings.
pub(crate) fn polygon_coords(poly: &impl PolygonTrait<T = f64>) -> Vec<Vec<[f64; 4]>> {
    poly.exterior()
        .into_iter()
        .chain(poly.interiors())
        .map(|ring| ring.coords().map(|c| ordinates(&c)).collect())
        .collect()
}

/// A heterogeneous array built from `geo` geometries, for the kernels that read
/// through `geometry_to_geo` and do not care about the concrete array type.
pub(crate) fn geometry_array(geoms: Vec<Option<geo::Geometry>>) -> impl GeoArrowArray {
    let typ = GeometryType::new(Default::default()).with_coord_type(CoordType::Interleaved);
    GeometryBuilder::from_nullable_geometries(&geoms, typ)
        .unwrap()
        .finish()
}

/// The `i`th value of a PolygonArray as a `geo` polygon.
pub(crate) fn polygon_at(result: &PolygonArray, i: usize) -> geo::Polygon {
    result.iter().nth(i).unwrap().unwrap().unwrap().to_polygon()
}

/// Each row as a `geo` geometry, for a kernel whose output type follows the input.
pub(crate) fn read_geoms<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
) -> Vec<Option<geo::Geometry>> {
    array
        .iter()
        .map(|item| item.map(|geom| geometry_to_geo(&geom.unwrap()).unwrap()))
        .collect()
}
