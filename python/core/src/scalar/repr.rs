use crate::error::PyGeoArrowResult;
use crate::scalar::*;
use geoarrow::algorithm::native::bounding_rect::{
    bounding_rect_geometry, bounding_rect_geometry_collection, bounding_rect_linestring,
    bounding_rect_multilinestring, bounding_rect_multipoint, bounding_rect_multipolygon,
    bounding_rect_point, bounding_rect_polygon,
};
use geoarrow::error::GeoArrowError;
use geozero::svg::SvgWriter;
use geozero::{FeatureProcessor, GeozeroGeometry};
use pyo3::exceptions::PyIOError;
use pyo3::prelude::*;

macro_rules! impl_repr_svg {
    ($struct_name:ident, $geoarrow_scalar:ty, $bounding_rect_fn:ident) => {
        #[pymethods]
        impl $struct_name {
            /// Render as SVG
            pub fn _repr_svg_(&self) -> PyGeoArrowResult<String> {
                let scalar = <$geoarrow_scalar>::from(&self.0);
                let ([mut min_x, mut min_y], [mut max_x, mut max_y]) = $bounding_rect_fn(&scalar);

                let mut svg_data = Vec::new();
                // Passing `true` to `invert_y` is necessary to match Shapely's _repr_svg_
                let mut svg = SvgWriter::new(&mut svg_data, true);

                // Expand box by 10% for readability
                min_x -= (max_x - min_x) * 0.05;
                min_y -= (max_y - min_y) * 0.05;
                max_x += (max_x - min_x) * 0.05;
                max_y += (max_y - min_y) * 0.05;

                svg.set_dimensions(min_x, min_y, max_x, max_y, 100, 100);

                // This sequence is necessary so that the SvgWriter writes the header. See
                // https://github.com/georust/geozero/blob/6c820ad7a0cac8c864058c783f548407427712d3/geozero/src/svg/mod.rs#L51-L58
                svg.dataset_begin(None)
                    .map_err(GeoArrowError::GeozeroError)?;
                svg.feature_begin(0).map_err(GeoArrowError::GeozeroError)?;
                scalar
                    .process_geom(&mut svg)
                    .map_err(GeoArrowError::GeozeroError)?;
                svg.feature_end(0).map_err(GeoArrowError::GeozeroError)?;
                svg.dataset_end().map_err(GeoArrowError::GeozeroError)?;

                let string = String::from_utf8(svg_data)
                    .map_err(|err| PyIOError::new_err(err.to_string()))?;
                Ok(string)
            }
        }
    };
}

impl_repr_svg!(Point, geoarrow::scalar::Point, bounding_rect_point);
impl_repr_svg!(
    LineString,
    geoarrow::scalar::LineString<i32>,
    bounding_rect_linestring
);
impl_repr_svg!(
    Polygon,
    geoarrow::scalar::Polygon<i32>,
    bounding_rect_polygon
);
impl_repr_svg!(
    MultiPoint,
    geoarrow::scalar::MultiPoint<i32>,
    bounding_rect_multipoint
);
impl_repr_svg!(
    MultiLineString,
    geoarrow::scalar::MultiLineString<i32>,
    bounding_rect_multilinestring
);
impl_repr_svg!(
    MultiPolygon,
    geoarrow::scalar::MultiPolygon<i32>,
    bounding_rect_multipolygon
);
impl_repr_svg!(
    Geometry,
    geoarrow::scalar::Geometry<i32>,
    bounding_rect_geometry
);
impl_repr_svg!(
    GeometryCollection,
    geoarrow::scalar::GeometryCollection<i32>,
    bounding_rect_geometry_collection
);
// impl_repr_svg!(
//     WKB,
//     geoarrow::scalar::WKB<i32>,
//     bounding_rect_geometry
// );
// impl_repr_svg!(Rect, geoarrow::scalar::Rect, bounding_rect_rect);
