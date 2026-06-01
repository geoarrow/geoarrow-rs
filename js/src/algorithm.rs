use std::sync::Arc;

use arrow_wasm::data::Data;
use geoarrow_schema::CoordType;
use wasm_bindgen::prelude::*;

use crate::data::JsGeoArrowData;
use crate::error::WasmResult;

macro_rules! scalar_unary {
    ($($(#[$doc:meta])* $js:ident => $rust:ident;)*) => {$(
        $(#[$doc])*
        #[wasm_bindgen(js_name = $js)]
        pub fn $rust(input: &JsGeoArrowData) -> WasmResult<Data> {
            Ok(Data::from_array(geoarrow_expr_geo::$rust(
                input.inner().as_ref(),
            )?))
        }
    )*};
}

scalar_unary! {
    /// Compute the area of each geometry, negative for a clockwise ring
    signedArea => signed_area;
    /// Compute the area of each geometry, whichever way its rings wind
    unsignedArea => unsigned_area;
    /// Compute the planar length of each geometry
    euclideanLength => euclidean_length;
    /// Compute the geodesic length of each geometry
    ///
    /// The geodesic, great circle and rhumb line lengths read each coordinate as
    /// a longitude and a latitude in degrees, give a length in meters, and give
    /// zero for a geometry with no line in it
    geodesicLength => geodesic_length;
    /// Compute the great circle length of each geometry
    haversineLength => haversine_length;
    /// Compute the rhumb line length of each geometry
    rhumbLength => rhumb_length;
    /// Compute the spherical area of each geometry, negative for a clockwise ring
    ///
    /// The signed and unsigned variants read each coordinate as a longitude and
    /// a latitude in degrees and give an area in square meters
    chamberlainDuquetteSignedArea => chamberlain_duquette_signed_area;
    /// Compute the spherical area of each geometry, whichever way its rings wind
    chamberlainDuquetteUnsignedArea => chamberlain_duquette_unsigned_area;
    /// Compute the topological dimension of each geometry
    ///
    /// The result is `-1` for an empty geometry, `0` for a point, `1` for a line
    /// and `2` for an area
    dimensions => dimensions;
    /// Test whether each geometry is empty
    isEmpty => is_empty;
}

macro_rules! scalar_binary {
    ($($(#[$doc:meta])* $js:ident => $rust:ident;)*) => {$(
        $(#[$doc])*
        ///
        /// The two arrays must have the same length
        #[wasm_bindgen(js_name = $js)]
        pub fn $rust(left: &JsGeoArrowData, right: &JsGeoArrowData) -> WasmResult<Data> {
            Ok(Data::from_array(geoarrow_expr_geo::$rust(
                left.inner().as_ref(),
                right.inner().as_ref(),
            )?))
        }
    )*};
}

scalar_binary! {
    /// Test whether the left geometry of each pair contains the right one
    contains => contains;
    /// Test whether each pair of geometries shares any point
    intersects => intersects;
    /// Compute the planar distance between each pair of geometries
    euclideanDistance => euclidean_distance;
    /// Compute the planar Hausdorff distance between each pair of geometries
    hausdorffDistance => hausdorff_distance;
    /// Compute the planar Frechet distance between each pair of line strings
    ///
    /// A pair in which either side is not a line string gives a null row
    frechetDistance => frechet_distance;
    /// Find how far along each left line its paired right point lies, as a
    /// fraction of the arc length
    ///
    /// A geometry which is not a line, or a query which is not a point, gives a
    /// null row
    lineLocatePoint => line_locate_point;
}

macro_rules! geometry_unary {
    ($($(#[$doc:meta])* $js:ident => $rust:ident;)*) => {$(
        $(#[$doc])*
        #[wasm_bindgen(js_name = $js)]
        pub fn $rust(input: &JsGeoArrowData) -> WasmResult<JsGeoArrowData> {
            Ok(JsGeoArrowData::from_arc(Arc::new(geoarrow_expr_geo::$rust(
                input.inner().as_ref(),
                CoordType::Interleaved,
            )?)))
        }
    )*};
}

geometry_unary! {
    /// Compute the center of mass of each geometry
    centroid => centroid;
    /// Compute a point inside each geometry
    ///
    /// Unlike the centroid, this point never falls outside a concave shape
    interiorPoint => interior_point;
    /// Compute the smallest rectangle that holds each geometry, at any angle
    minimumRotatedRect => minimum_rotated_rect;
    /// Compute the envelope of each geometry, as a two dimensional Polygon array
    ///
    /// A geometry with no coordinates has no envelope, thus its row is null
    boundingRect => bounding_rect;
    /// Compute the midpoint of the envelope of each geometry, which is not the
    /// centroid, as a two dimensional Point array
    ///
    /// A geometry with no coordinates has no envelope, thus its row is null
    center => center;
}

macro_rules! geometry_binary {
    ($($(#[$doc:meta])* $js:ident => $rust:ident;)*) => {$(
        $(#[$doc])*
        ///
        /// The two arrays must have the same length
        #[wasm_bindgen(js_name = $js)]
        pub fn $rust(left: &JsGeoArrowData, right: &JsGeoArrowData) -> WasmResult<JsGeoArrowData> {
            Ok(JsGeoArrowData::from_arc(Arc::new(geoarrow_expr_geo::$rust(
                left.inner().as_ref(),
                right.inner().as_ref(),
                CoordType::Interleaved,
            )?)))
        }
    )*};
}

geometry_binary! {
    /// Find the point of each left geometry closest to its paired right query
    /// point, as a two dimensional Point array
    ///
    /// A query that is not a point, or two or more equally close answers, gives
    /// a null row
    closestPoint => closest_point;
    /// Overlay each pair of geometries, keeping the region in both
    ///
    /// The four overlays give a two dimensional MultiPolygon array, and give a
    /// null row for an operand that is neither a polygon nor a multipolygon
    intersection => intersection;
    /// Overlay each pair of geometries, keeping the region in either
    union => union;
    /// Overlay each pair of geometries, keeping the region of `left` outside `right`
    difference => difference;
    /// Overlay each pair of geometries, keeping the region in exactly one
    xor => xor;
}

macro_rules! epsilon_unary {
    ($($(#[$doc:meta])* $js:ident => $rust:ident;)*) => {$(
        $(#[$doc])*
        #[wasm_bindgen(js_name = $js)]
        pub fn $rust(input: &JsGeoArrowData, epsilon: f64) -> WasmResult<JsGeoArrowData> {
            Ok(JsGeoArrowData::from_arc(geoarrow_expr_geo::$rust(
                input.inner().as_ref(),
                epsilon,
            )?))
        }
    )*};
}

epsilon_unary! {
    /// Simplify each geometry by the Ramer-Douglas-Peucker algorithm
    ///
    /// Each ring of a polygon is simplified on its own, which can give an
    /// invalid polygon; an `epsilon` of zero or less changes nothing
    simplify => simplify;
    /// Simplify each geometry by the Visvalingam-Whyatt algorithm, which drops the
    /// vertex of the smallest triangle until every triangle is larger than `epsilon`
    ///
    /// This does not keep the topology: two adjacent polygons can come to
    /// overlap or to leave a gap
    simplifyVw => simplify_vw;
    /// Simplify each geometry by the Visvalingam-Whyatt algorithm, keeping the
    /// topology
    ///
    /// Slower than `simplifyVw`, but adds no self intersection
    simplifyVwPreserve => simplify_vw_preserve;
}

macro_rules! param_geometry_unary {
    ($($(#[$doc:meta])* $js:ident => $rust:ident($param:ident);)*) => {$(
        $(#[$doc])*
        #[wasm_bindgen(js_name = $js)]
        pub fn $rust(input: &JsGeoArrowData, $param: f64) -> WasmResult<JsGeoArrowData> {
            Ok(JsGeoArrowData::from_arc(Arc::new(geoarrow_expr_geo::$rust(
                input.inner().as_ref(),
                $param,
                CoordType::Interleaved,
            )?)))
        }
    )*};
}

param_geometry_unary! {
    /// Buffer each geometry, as a two dimensional MultiPolygon array
    ///
    /// A positive `distance` dilates and a negative one erodes
    buffer => buffer(distance);
    /// Compute the concave hull of each geometry, as a two dimensional Polygon array
    ///
    /// A smaller `concavity` traces a tighter boundary and a larger one
    /// approaches the convex hull; it must be finite and not negative. A point
    /// or a geometry collection gives a null row
    concaveHull => concave_hull(concavity);
    /// Find the point `fraction` of the way along each line, by arc length, as a
    /// two dimensional Point array
    ///
    /// A `fraction` outside `[0, 1]` holds at the nearer end; a geometry which
    /// is not a line gives a null row
    lineInterpolatePoint => line_interpolate_point(fraction);
}

/// Compute the convex hull of each geometry, wound counter clockwise
#[wasm_bindgen(js_name = convexHull)]
pub fn convex_hull(input: &JsGeoArrowData) -> WasmResult<JsGeoArrowData> {
    Ok(JsGeoArrowData::from_arc(geoarrow_expr_geo::convex_hull(
        input.inner().as_ref(),
        CoordType::Interleaved,
    )?))
}

/// Add vertices to each geometry until no two adjacent vertices are more than
/// `max_distance` apart, in the Euclidean metric
///
/// `max_distance` must be finite and greater than zero. A new vertex has no Z
/// or M of its own, thus the output is two dimensional
#[wasm_bindgen(js_name = densify)]
pub fn densify(input: &JsGeoArrowData, max_distance: f64) -> WasmResult<JsGeoArrowData> {
    Ok(JsGeoArrowData::from_arc(geoarrow_expr_geo::densify(
        input.inner().as_ref(),
        max_distance,
        CoordType::Interleaved,
    )?))
}

/// Cut the corners of each geometry `n_iterations` times, by Chaikin's algorithm
///
/// A cut corner is a new vertex with no Z or M of its own, thus the output is
/// two dimensional
#[wasm_bindgen(js_name = chaikinSmoothing)]
pub fn chaikin_smoothing(
    input: &JsGeoArrowData,
    n_iterations: usize,
) -> WasmResult<JsGeoArrowData> {
    Ok(JsGeoArrowData::from_arc(
        geoarrow_expr_geo::chaikin_smoothing(
            input.inner().as_ref(),
            n_iterations,
            CoordType::Interleaved,
        )?,
    ))
}

/// Rotate each geometry counter clockwise about `(origin_x, origin_y)`
///
/// The four transforms read x and y only, thus Z and M pass through and each
/// array keeps its own geometry type
#[wasm_bindgen(js_name = rotate)]
pub fn rotate(
    input: &JsGeoArrowData,
    degrees: f64,
    origin_x: f64,
    origin_y: f64,
) -> WasmResult<JsGeoArrowData> {
    Ok(JsGeoArrowData::from_arc(geoarrow_expr_geo::rotate(
        input.inner().as_ref(),
        degrees,
        origin_x,
        origin_y,
        CoordType::Interleaved,
    )?))
}

/// Scale each geometry about `(origin_x, origin_y)`
#[wasm_bindgen(js_name = scale)]
pub fn scale(
    input: &JsGeoArrowData,
    x_factor: f64,
    y_factor: f64,
    origin_x: f64,
    origin_y: f64,
) -> WasmResult<JsGeoArrowData> {
    Ok(JsGeoArrowData::from_arc(geoarrow_expr_geo::scale(
        input.inner().as_ref(),
        x_factor,
        y_factor,
        origin_x,
        origin_y,
        CoordType::Interleaved,
    )?))
}

/// Skew each geometry about `(origin_x, origin_y)`
#[wasm_bindgen(js_name = skew)]
pub fn skew(
    input: &JsGeoArrowData,
    x_degrees: f64,
    y_degrees: f64,
    origin_x: f64,
    origin_y: f64,
) -> WasmResult<JsGeoArrowData> {
    Ok(JsGeoArrowData::from_arc(geoarrow_expr_geo::skew(
        input.inner().as_ref(),
        x_degrees,
        y_degrees,
        origin_x,
        origin_y,
        CoordType::Interleaved,
    )?))
}

/// Translate each geometry by `(x_offset, y_offset)`
#[wasm_bindgen(js_name = translate)]
pub fn translate(
    input: &JsGeoArrowData,
    x_offset: f64,
    y_offset: f64,
) -> WasmResult<JsGeoArrowData> {
    Ok(JsGeoArrowData::from_arc(geoarrow_expr_geo::translate(
        input.inner().as_ref(),
        x_offset,
        y_offset,
        CoordType::Interleaved,
    )?))
}

/// Test each pair of geometries against a DE-9IM pattern
///
/// The two arrays must have the same length
///
/// @param pattern Nine characters from `{T, F, *, 0, 1, 2}`, one for each cell of
/// the matrix relating the interior, boundary and exterior of the two geometries.
#[wasm_bindgen(js_name = relateBoolean)]
pub fn relate_boolean(
    left: &JsGeoArrowData,
    right: &JsGeoArrowData,
    pattern: &str,
) -> WasmResult<Data> {
    if pattern.len() != 9
        || !pattern
            .bytes()
            .all(|b| matches!(b, b'T' | b'F' | b'*' | b'0' | b'1' | b'2'))
    {
        return Err(JsError::new(
            "DE-9IM pattern must be 9 characters from {T, F, *, 0, 1, 2}",
        ));
    }
    let pattern = pattern.to_string();
    let result = geoarrow_expr_geo::relate_boolean(
        left.inner().as_ref(),
        right.inner().as_ref(),
        // The check above rejects every malformed pattern, thus an error here is
        // a genuine non-match rather than a swallowed failure.
        move |m| m.matches(&pattern).unwrap_or(false),
    )?;
    Ok(Data::from_array(result))
}
