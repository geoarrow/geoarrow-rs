use crate::array::*;
use crate::chunked_array::*;
use crate::error::PyGeoArrowResult;
use crate::ffi::from_python::import_arrow_c_array;
use geoarrow::algorithm::geo::GeodesicArea;
use geoarrow::array::from_arrow_array;
use pyo3::prelude::*;

/// Determine the area of a geometry on an ellipsoidal model of the earth.
///
/// This uses the geodesic measurement methods given by [Karney (2013)].
///
/// ## Assumptions
///  - Polygons are assumed to be wound in a counter-clockwise direction
///    for the exterior ring and a clockwise direction for interior rings.
///    This is the standard winding for geometries that follow the Simple Feature standard.
///    Alternative windings may result in a negative area. See "Interpreting negative area values" below.
///  - Polygons are assumed to be smaller than half the size of the earth. If you expect to be dealing
///    with polygons larger than this, please use the `unsigned` methods.
///
/// ## Units
///
/// - return value: meter²
///
/// ## Interpreting negative area values
///
/// A negative value can mean one of two things:
/// 1. The winding of the polygon is in the clockwise direction (reverse winding). If this is the case, and you know the polygon is smaller than half the area of earth, you can take the absolute value of the reported area to get the correct area.
/// 2. The polygon is larger than half the planet. In this case, the returned area of the polygon is not correct. If you expect to be dealing with very large polygons, please use the `unsigned` methods.
///
/// [Karney (2013)]:  https://arxiv.org/pdf/1109.4448.pdf
///
/// Args:
///     input: input geometry array
///
/// Returns:
///     Array with output values.
#[pyfunction]
pub fn geodesic_area_signed(input: &PyAny) -> PyGeoArrowResult<Float64Array> {
    let (array, field) = import_arrow_c_array(input)?;
    let array = from_arrow_array(&array, &field)?;
    Ok(array.as_ref().geodesic_area_signed()?.into())
}

/// Determine the area of a geometry on an ellipsoidal model of the earth. Supports very large geometries that cover a significant portion of the earth.
///
/// This uses the geodesic measurement methods given by [Karney (2013)].
///
/// ## Assumptions
///  - Polygons are assumed to be wound in a counter-clockwise direction
///    for the exterior ring and a clockwise direction for interior rings.
///    This is the standard winding for geometries that follow the Simple Features standard.
///    Using alternative windings will result in incorrect results.
///
/// ## Units
///
/// - return value: meter²
///
/// [Karney (2013)]:  https://arxiv.org/pdf/1109.4448.pdf#[pyfunction]
///
/// Args:
///     input: input geometry array
///
/// Returns:
///     Array with output values.
#[pyfunction]
pub fn geodesic_area_unsigned(input: &PyAny) -> PyGeoArrowResult<Float64Array> {
    let (array, field) = import_arrow_c_array(input)?;
    let array = from_arrow_array(&array, &field)?;
    Ok(array.as_ref().geodesic_area_unsigned()?.into())
}

/// Determine the perimeter of a geometry on an ellipsoidal model of the earth.
///
/// This uses the geodesic measurement methods given by [Karney (2013)].
///
/// For a polygon this returns the sum of the perimeter of the exterior ring and interior rings.
/// To get the perimeter of just the exterior ring of a polygon, do `polygon.exterior().geodesic_length()`.
///
/// ## Units
///
/// - return value: meter
///
/// [Karney (2013)]:  https://arxiv.org/pdf/1109.4448.pdf
///
/// Returns:
///     Array with output values.
#[pyfunction]
pub fn geodesic_perimeter(input: &PyAny) -> PyGeoArrowResult<Float64Array> {
    let (array, field) = import_arrow_c_array(input)?;
    let array = from_arrow_array(&array, &field)?;
    Ok(array.as_ref().geodesic_perimeter()?.into())
}

macro_rules! impl_geodesic_area {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// Determine the area of a geometry on an ellipsoidal model of the earth.
            ///
            /// This uses the geodesic measurement methods given by [Karney (2013)].
            ///
            /// ## Assumptions
            ///  - Polygons are assumed to be wound in a counter-clockwise direction
            ///    for the exterior ring and a clockwise direction for interior rings.
            ///    This is the standard winding for geometries that follow the Simple Feature standard.
            ///    Alternative windings may result in a negative area. See "Interpreting negative area values" below.
            ///  - Polygons are assumed to be smaller than half the size of the earth. If you expect to be dealing
            ///    with polygons larger than this, please use the `unsigned` methods.
            ///
            /// ## Units
            ///
            /// - return value: meter²
            ///
            /// ## Interpreting negative area values
            ///
            /// A negative value can mean one of two things:
            /// 1. The winding of the polygon is in the clockwise direction (reverse winding). If this is the case, and you know the polygon is smaller than half the area of earth, you can take the absolute value of the reported area to get the correct area.
            /// 2. The polygon is larger than half the planet. In this case, the returned area of the polygon is not correct. If you expect to be dealing with very large polygons, please use the `unsigned` methods.
            ///
            /// [Karney (2013)]:  https://arxiv.org/pdf/1109.4448.pdf
            ///
            /// Returns:
            ///     Array with output values.
            pub fn geodesic_area_signed(&self) -> Float64Array {
                GeodesicArea::geodesic_area_signed(&self.0).into()
            }

            /// Determine the area of a geometry on an ellipsoidal model of the earth. Supports very large geometries that cover a significant portion of the earth.
            ///
            /// This uses the geodesic measurement methods given by [Karney (2013)].
            ///
            /// ## Assumptions
            ///  - Polygons are assumed to be wound in a counter-clockwise direction
            ///    for the exterior ring and a clockwise direction for interior rings.
            ///    This is the standard winding for geometries that follow the Simple Features standard.
            ///    Using alternative windings will result in incorrect results.
            ///
            /// ## Units
            ///
            /// - return value: meter²
            ///
            /// [Karney (2013)]:  https://arxiv.org/pdf/1109.4448.pdf
            ///
            /// Returns:
            ///     Array with output values.
            pub fn geodesic_area_unsigned(&self) -> Float64Array {
                GeodesicArea::geodesic_area_unsigned(&self.0).into()
            }

            /// Determine the perimeter of a geometry on an ellipsoidal model of the earth.
            ///
            /// This uses the geodesic measurement methods given by [Karney (2013)].
            ///
            /// For a polygon this returns the sum of the perimeter of the exterior ring and interior rings.
            /// To get the perimeter of just the exterior ring of a polygon, do `polygon.exterior().geodesic_length()`.
            ///
            /// ## Units
            ///
            /// - return value: meter
            ///
            /// [Karney (2013)]:  https://arxiv.org/pdf/1109.4448.pdf
            ///
            /// Returns:
            ///     Array with output values.
            pub fn geodesic_perimeter(&self) -> Float64Array {
                GeodesicArea::geodesic_perimeter(&self.0).into()
            }
        }
    };
}

impl_geodesic_area!(PointArray);
impl_geodesic_area!(LineStringArray);
impl_geodesic_area!(PolygonArray);
impl_geodesic_area!(MultiPointArray);
impl_geodesic_area!(MultiLineStringArray);
impl_geodesic_area!(MultiPolygonArray);
impl_geodesic_area!(MixedGeometryArray);
impl_geodesic_area!(GeometryCollectionArray);

macro_rules! impl_chunked {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// Determine the area of a geometry on an ellipsoidal model of the earth.
            ///
            /// This uses the geodesic measurement methods given by [Karney (2013)].
            ///
            /// ## Assumptions
            ///  - Polygons are assumed to be wound in a counter-clockwise direction
            ///    for the exterior ring and a clockwise direction for interior rings.
            ///    This is the standard winding for geometries that follow the Simple Feature standard.
            ///    Alternative windings may result in a negative area. See "Interpreting negative area values" below.
            ///  - Polygons are assumed to be smaller than half the size of the earth. If you expect to be dealing
            ///    with polygons larger than this, please use the `unsigned` methods.
            ///
            /// ## Units
            ///
            /// - return value: meter²
            ///
            /// ## Interpreting negative area values
            ///
            /// A negative value can mean one of two things:
            /// 1. The winding of the polygon is in the clockwise direction (reverse winding). If this is the case, and you know the polygon is smaller than half the area of earth, you can take the absolute value of the reported area to get the correct area.
            /// 2. The polygon is larger than half the planet. In this case, the returned area of the polygon is not correct. If you expect to be dealing with very large polygons, please use the `unsigned` methods.
            ///
            /// [Karney (2013)]:  https://arxiv.org/pdf/1109.4448.pdf
            ///
            /// Returns:
            ///     Array with output values.
            pub fn geodesic_area_signed(&self) -> PyGeoArrowResult<ChunkedFloat64Array> {
                Ok(GeodesicArea::geodesic_area_signed(&self.0)?.into())
            }

            /// Determine the area of a geometry on an ellipsoidal model of the earth. Supports very large geometries that cover a significant portion of the earth.
            ///
            /// This uses the geodesic measurement methods given by [Karney (2013)].
            ///
            /// ## Assumptions
            ///  - Polygons are assumed to be wound in a counter-clockwise direction
            ///    for the exterior ring and a clockwise direction for interior rings.
            ///    This is the standard winding for geometries that follow the Simple Features standard.
            ///    Using alternative windings will result in incorrect results.
            ///
            /// ## Units
            ///
            /// - return value: meter²
            ///
            /// [Karney (2013)]:  https://arxiv.org/pdf/1109.4448.pdf
            ///
            /// Returns:
            ///     Array with output values.
            pub fn geodesic_area_unsigned(&self) -> PyGeoArrowResult<ChunkedFloat64Array> {
                Ok(GeodesicArea::geodesic_area_unsigned(&self.0)?.into())
            }

            /// Determine the perimeter of a geometry on an ellipsoidal model of the earth.
            ///
            /// This uses the geodesic measurement methods given by [Karney (2013)].
            ///
            /// For a polygon this returns the sum of the perimeter of the exterior ring and interior rings.
            /// To get the perimeter of just the exterior ring of a polygon, do `polygon.exterior().geodesic_length()`.
            ///
            /// ## Units
            ///
            /// - return value: meter
            ///
            /// [Karney (2013)]:  https://arxiv.org/pdf/1109.4448.pdf
            ///
            /// Returns:
            ///     Array with output values.
            pub fn geodesic_perimeter(&self) -> PyGeoArrowResult<ChunkedFloat64Array> {
                Ok(GeodesicArea::geodesic_perimeter(&self.0)?.into())
            }
        }
    };
}

impl_chunked!(ChunkedPointArray);
impl_chunked!(ChunkedLineStringArray);
impl_chunked!(ChunkedPolygonArray);
impl_chunked!(ChunkedMultiPointArray);
impl_chunked!(ChunkedMultiLineStringArray);
impl_chunked!(ChunkedMultiPolygonArray);
impl_chunked!(ChunkedMixedGeometryArray);
impl_chunked!(ChunkedGeometryCollectionArray);
